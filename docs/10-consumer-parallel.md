# 10 · Scaling and parallelism

**Demo:** `consumer-parallel` · [ConsumerParallelDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerParallelDemo.java)

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

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="consumer-parallel"
```

Arguments: `records=6000`, `work-ms=5`, `modes=1,2,5`.

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

## When to use what

| Situation | Mode |
|---|---|
| handler is fast (< 1 ms), fetch is the limit | 1, then tune chapter 07 |
| handler is slow, you can add partitions and processes | 2 (the operational default: scale the deployment, not the code) |
| handler is slow, partition count is fixed, ordering matters | 5 (or 3 if the handler is predictable and `max.poll.interval.ms` has room) |
| records independent, throughput above all | 4 with a bounded semaphore around the downstream call |
| need a library instead of hand-rolled code | Spring Kafka `concurrency` (= mode 2 in one JVM), Confluent Parallel Consumer (≈ mode 5 with key ordering), or a share group (chapter 11) |
