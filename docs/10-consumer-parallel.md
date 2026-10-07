# 10 · Scaling and parallelism

> **Level:** Practitioner · **Read first:** [07](07-consumer-fetch.md), [09](09-consumer-rebalance.md) · **Time:** ~5 min read, ~2 min run · [Glossary](glossary.md)
>
> **Demo:** `consumer-parallel` (`./demo 10`) · [ConsumerParallelDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerParallelDemo.java) · **Recipe:** [PartitionedWorkerConsumer.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/PartitionedWorkerConsumer.java) · **In Spring:** [17](17-spring-concurrency-batch.md)
>
> **In one sentence:** When the handler is the bottleneck, one consumer with a sequential worker per partition, `pause()`/`resume()` and watermark commits matched six consumers (628 vs 649 records/s) and kept per-partition ordering.

## The problem

Fetching is rarely the bottleneck (chapter 07 pulled 100 000+ records/s through one consumer). The
handler is: a database write, an HTTP call, a few milliseconds of CPU per record. With 5 ms per record a
sequential consumer tops out at 200 records/s no matter what you tune. This chapter is about getting past
that without breaking the two rules that keep a consumer correct.

## The two rules

1. **Keep calling `poll()`** within `max.poll.interval.ms`, whatever the handler is doing. A poll thread
   that blocks on work is a consumer that gets kicked out (chapter 07).
2. **Commit only what is finished**, per partition, in order. The committed offset promises "everything
   below this is done"; a commit ahead of unfinished work is at-most-once by accident (chapter 08).

## The options

| Mode | Consumers | Workers | Ordering | Notes |
|---|---|---|---|---|
| 1 sequential | 1 | 1 | per partition | the baseline: `records/s = 1000 / ms-per-record` |
| 2 one consumer per partition | = partitions | = partitions | per partition | the classic answer; capped by the partition count, costs a process/thread + connections per consumer |
| 3 per-partition tasks per poll | 1 | ≤ partitions | per partition | fan out each poll's records by partition, wait for all, commit. Simple, but the poll thread waits: `(records per partition per poll × ms) < max.poll.interval.ms` |
| 4 one task per record | 1 | unbounded (virtual threads) | **none** | fastest, only for independent, idempotent records |
| 5 asynchronous pipeline | 1 | = partitions | per partition | one sequential worker per partition, `pause()`/`resume()` for back-pressure, per-partition watermark commits. The production shape |

Virtual threads (Java 21+) make workers free to create; the interesting limits move to the systems the
handler talks to.

## The code that matters

Mode 5, the production shape, is [PartitionedWorkerConsumer.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/PartitionedWorkerConsumer.java).
The poll loop hands records out and never waits:

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/PartitionedWorkerConsumer.java -->
```java
ConsumerRecords<K, V> batch = consumer.poll(timeout);
for (ConsumerRecord<K, V> record : batch) {
    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
    inFlight.incrementAndGet();
    // One single-threaded executor per partition: FIFO per partition, so order is kept.
    workers.computeIfAbsent(partition, p -> Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()))
            .submit(() -> process(partition, record));
}
// Back-pressure: stop fetching while the workers are behind, but keep calling poll() so the group still sees
// this member alive (max.poll.interval.ms).
if (!paused && inFlight.get() > pauseAbove) {
    consumer.pause(consumer.assignment());
    paused = true;
    pauses++;
} else if (paused && inFlight.get() < resumeBelow) {
    consumer.resume(consumer.assignment());
    paused = false;
}
Map<TopicPartition, OffsetAndMetadata> offsets = watermarks();
if (!offsets.isEmpty()) {
    consumer.commitAsync(offsets, null);   // a lost async commit is harmless: the next one carries a higher watermark
}
```

and a worker only moves its partition's watermark once a record is done:

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/PartitionedWorkerConsumer.java -->
```java
private void process(TopicPartition partition, ConsumerRecord<K, V> record) {
    try {
        if (failed.containsKey(partition)) {
            return;   // an earlier record of this partition failed: go no further, or the order would break
        }
        handler.handle(record);
        // Only this partition's worker writes this entry, one record at a time: the watermark only ever grows.
        nextToCommit.computeIfAbsent(partition, p -> new AtomicLong()).set(record.offset() + 1);
    } catch (Exception e) {
        failed.putIfAbsent(partition, new Failure(record.offset(), e));
    } finally {
        inFlight.decrementAndGet();
    }
}
```

- **One consumer matched six** (628 vs 649 records/s below) because six workers run at once and the poll thread
  never blocks; ordering per partition is kept, unlike mode 4.
- **Commit the watermarks, never the position.** `close()` finishes the queued work and then commits
  `watermarks()`; the no-arg `commitSync()` would commit everything fetched, done or not.
- **`PartitionedWorkerConsumer.config()`** sets `max.partition.fetch.bytes=16K`: a poll returns records partition by
  partition, and small per-partition fetches make one poll feed every worker.
- **Rebalances and failures are handled**: revoked partitions are finished and committed in the rebalance listener
  (`pipeline.subscribe(topics)`), a failing record stops its partition there and the next `pollOnce` reports it.
  Neither happens in the demo; `PartitionedWorkerConsumerTest` covers both.

The demo's mode 5 is this class with `r -> work(r, workMs)` as the handler; modes 1–4 show the alternatives, and
everything else in [ConsumerParallelDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerParallelDemo.java)
is measurement.

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="consumer-parallel"
```

Arguments: `records=6000`, `work-ms=5`, `modes=1,2,5` (a subset of the five modes; all of them run by default).

## What you should see

6 000 records, 5 ms of simulated work each (30 s of pure processing), 6 partitions:

```
| mode                           | consumers | workers             | elapsed s | records/s | ordering      | commit safety                                       |
|                   1 sequential |         1 |                   1 |     34.21 |       175 | per partition | commit after batch: at-least-once                   |
|       2 consumer per partition |         6 |                   6 |      9.25 |       649 | per partition | same as 1, per consumer                             |
| 3 per-partition tasks per poll |         1 |                   6 |     27.75 |       216 | per partition | commit after whole batch done                       |
|          4 one task per record |         1 | unbounded (virtual) |      0.66 |      9086 | NONE          | commit after whole batch done                       |
|   5 async pipeline (paused 2x) |         1 |                   6 |      9.55 |       628 | per partition | watermark commits: at-least-once, poll never blocks |
```

## Reading the numbers

- **Mode 1 is the arithmetic.** 6 000 records × 5.7 ms (what `Thread.sleep(5)` really costs) = 34 s, one
  thread: ~175 records/s.
- **Mode 2 divides by the partition count**, and no further: a 7th consumer would sit idle. The 9.3 s
  include ~4 s of group formation (six members joining a `classic` group one after another); the steady
  state is close to the theoretical 6×. It also multiplies connections, metadata traffic and rebalance
  participants.
- **Mode 3 barely helps (1.2×), and that is the lesson.** One `poll()` hands back records partition by
  partition: it drains what the fetcher buffered for one partition before touching the next, so most polls
  span only one or two partitions and only one or two workers get anything to do. The demo already caps
  `max.partition.fetch.bytes` at 16 KB to force some interleaving; with the default 1 MB per partition the
  fan-out would be exactly zero. Per-poll fan-out only pays off when polls span many partitions.
- **Mode 5 matches six consumers from one process** (628 vs 649 records/s), because work queues up per
  partition *across* polls: a worker that finished its slice does not wait for the next poll to bring more.
  The pause/resume pair kept the buffered work bounded (paused twice), and every loop committed only the
  per-partition watermark.
- **Mode 4 is bounded only by how many records a poll returns** and by the downstream system: 9 000
  records/s here, 50× the baseline. Ordering is gone: two updates for the same key can complete in either
  order. Correct only when records are independent (or the handler is a commutative upsert).
- **How mode 5 works**: records are handed to a per-partition single-thread executor (FIFO per partition =
  Kafka's ordering), the loop pauses fetching when too much work is in flight and resumes when it drains
  (`pause`/`resume` keep the consumer polling, so it stays in the group), and every loop commits the
  per-partition "next offset to process" watermark. It is at-least-once with a bounded redelivery window
  and never blocks on work.

## Key takeaways

- **The partition is the unit of parallelism in a group.** Six consumers on six partitions reached 649 records/s;
  a seventh consumer would sit idle.
- **Inside one consumer: keep polling, commit only finished work.** A worker per partition, `pause()`/`resume()` and
  watermark commits reached 628 records/s with per-partition ordering kept.
- **Per-poll fan-out barely helps; per-record fan-out drops ordering.** Mode 3 gained 1.2× because polls span one or
  two partitions; mode 4 ran 9 000 records/s, unordered.

## When to use what

| Situation | Mode |
|---|---|
| handler is fast (< 1 ms), fetch is the limit | 1, then tune chapter 07 |
| handler is slow, you can add partitions and processes | 2 (the operational default: scale the deployment, not the code) |
| handler is slow, partition count is fixed, ordering matters | 5 (or 3 if the handler is predictable and `max.poll.interval.ms` has room) |
| records independent, throughput above all | 4 with a bounded semaphore around the downstream call |
| need a library instead of hand-rolled code | Spring Kafka `concurrency` (= mode 2 in one JVM), Confluent Parallel Consumer (≈ mode 5 with key ordering), or a share group (chapter 11) |

---

← [09 · Group protocol and rebalancing](09-consumer-rebalance.md) · [Index](README.md) · [11 · Queues for Kafka: share groups](11-consumer-share-groups.md) →
