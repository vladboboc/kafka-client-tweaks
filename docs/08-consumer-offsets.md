# 08 · Offsets and delivery guarantees

> **Level:** Essentials · **Read first:** [07](07-consumer-fetch.md) · **Time:** ~5 min read, ~1 min run · [Glossary](glossary.md)
>
> **Demo:** `consumer-offsets` (`./demo 08`) · [ConsumerOffsetsDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerOffsetsDemo.java) · **Recipes:** [AtLeastOnceConsumer.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/AtLeastOnceConsumer.java), [Replay.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/Replay.java) · **In Spring:** [16](16-spring-listeners-acks.md), [18](18-spring-error-handling-retry.md)
>
> **In one sentence:** The committed offset is a consumer's only durable state: commit before handling lost 266 records in a crash, after handling redelivered 234, and after handling plus an idempotent handler lost and duplicated nothing.

## The problem

A consumer has exactly one piece of durable state: the committed offset per partition, stored in
`__consumer_offsets`. Everything about "did we process this record once, twice or never" comes down to
*when* that number is written relative to the work. Kafka does not know what your handler did; it only
knows what you committed.

## The knobs

| Config / API | Default | Meaning |
|---|---|---|
| `enable.auto.commit` | `true` | the consumer commits the offsets of the *previous* poll inside the *next* poll, once `auto.commit.interval.ms` has passed |
| `auto.commit.interval.ms` | 5000 | how much work is at most redelivered after a crash, with auto-commit |
| `commitSync()` / `commitAsync()` | – | manual commits of the current position (or of an explicit offset map). `commitSync` blocks and retries; `commitAsync` returns immediately and does not retry |
| `auto.offset.reset` | `latest` | where a group **without** committed offsets starts: `earliest`, `latest` or `none` (throw). Irrelevant once offsets exist |
| `seek`, `seekToBeginning`, `seekToEnd`, `offsetsForTimes` | – | set the position by hand: replay, skip, time travel |
| `isolation.level` | `read_uncommitted` | chapter 06 |

## The code that matters

Where the `commitSync()` sits relative to the handler, from
[AtLeastOnceConsumer.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/AtLeastOnceConsumer.java)
(with `enable.auto.commit=false`, `AtLeastOnceConsumer.manualCommits()`):

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/AtLeastOnceConsumer.java -->
```java
public int pollOnce(Duration timeout) throws Exception {
    ConsumerRecords<K, V> batch = consumer.poll(timeout);
    if (batch.isEmpty()) {
        return 0;
    }
    if (commitPoint == CommitPoint.BEFORE_HANDLING) {
        consumer.commitSync();   // the position is already past this batch: this commits "we will have handled these"
    }
    for (ConsumerRecord<K, V> record : batch) {
        handler.handle(record);
    }
    if (commitPoint == CommitPoint.AFTER_HANDLING) {
        consumer.commitSync();   // commits the position after the batch: "these are done"
    }
    return batch.count();
}
// ...
public static <K, V> RecordHandler<K, V> skipDuplicates(Function<ConsumerRecord<K, V>, String> idOf,
                                                        Predicate<String> alreadyHandled, RecordHandler<K, V> handler) {
    return record -> {
        if (!alreadyHandled.test(idOf.apply(record))) {
            handler.handle(record);
        }
    };
}
```

- **`BEFORE_HANDLING` lost 266 records**, **`AFTER_HANDLING` processed 234 twice**, and
  **`AFTER_HANDLING` + `skipDuplicates(...)`** neither. A handler that throws commits nothing of its batch, which is
  what makes the redelivery happen.
- **`skipDuplicates` only works if the handler stores the id in the same transaction as its side effect**
  (a processed-events table, an upsert): the lookup is only as durable as that write.
- **Replay** is a few lines each in [Replay.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/recipe/Replay.java):
  `fromTime(consumer, instant)` (`offsetsForTimes`, then seek), `lastRecords(consumer, n)` and `rewindGroup(consumer)`.

The demo's `consume()` runs each strategy through `AtLeastOnceConsumer`, the crash being a handler that throws after
record 1234, and its part 4 calls `Replay`. Everything else in
[ConsumerOffsetsDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerOffsetsDemo.java) is
measurement. The loop is unit-tested with `MockConsumer` (`AtLeastOnceConsumerTest`).

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="consumer-offsets"
```

Arguments: `records=3000`, `crash-at=1234`.

## What you should see

**1. One crash, three commit strategies.** The consumer processes records and "crashes" after record
1234, in the middle of a 500-record batch. A second instance of the same group finishes the topic:

```
| strategy                 | processed (run 1 + run 2) | distinct records | missing | duplicates (handler saw twice) |
| commit before processing |               1234 + 1500 |             2734 |     266 |                              0 |
| commit after processing  |               1234 + 2000 |             3000 |       0 |                            234 |
| commit after idempotent  |               1234 + 1766 |             3000 |       0 |                              0 |
```

**2. Auto-commit timing.** A consumer that processes 100 records per poll, ~10 polls/s, while the demo
samples `position()` and `committed()` every second:

```
| t (s) | position (sum over partitions) | committed (sum) | uncommitted = redelivered on crash |
|     0 |                            100 |               0 |                                100 |
|     1 |                           1000 |               0 |                               1000 |
|     2 |                           1900 |               0 |                               1900 |
|     3 |                           2900 |               0 |                               2900 |
|     4 |                           3000 |               0 |                               3000 |
|     5 |                           3000 |            3000 |                                  0 |
```

**3. `auto.offset.reset`** for three fresh groups on a 3 000-record topic:

```
| auto.offset.reset | first poll returned           | position after        |
| earliest          |                   500 records |                   500 |
| latest            |                     0 records | 3000 (= end of topic) |
| none              | NoOffsetForPartitionException |                     - |
```

**4. Seeking:** `seekToBeginning` → 3 000 readable; `seek(end − 100)` on each of 3 partitions → 300;
`offsetsForTimes(one hour ago)` → 3 000 (everything is newer); `offsetsForTimes(now)` → 0; and
`commitSync(offset 0 for every partition)` rewinds the whole group for its next start.

## Reading the numbers

- **Commit before processing = at-most-once.** The crash lost 266 records: the batch was committed as
  "done" while 266 of its records were still unprocessed. Nobody will ever see them again unless someone
  seeks back.
- **Commit after processing = at-least-once.** The 234 records processed before the crash but after the
  last commit came back in run 2. This is the default contract of Kafka consumption; everything downstream
  must tolerate it.
- **At-least-once + an idempotent handler = effectively-once.** The same 234 redeliveries arrive, and the
  handler recognises them: run 2 handles 1 766 records, not 2 000. Its memory must survive the crash: a unique
  constraint or upsert in the database you write to, a processed-ids table with the same transaction as the
  business write (the inbox pattern), a conditional `PUT`. An in-memory set would not have helped.
- **Auto-commit is at-least-once *only* while processing is synchronous in the poll loop.** The
  committed offset trails by up to 5 s (t=0..4 above), then jumps. Hand records to another thread and keep
  polling, and the next poll commits offsets for work that has not happened: at-most-once by accident.
  Chapter 10 shows how to do that correctly.
- **A clean `close()` commits** (with auto-commit on) and leaves the group; an orderly shutdown loses nothing
  and duplicates nothing. Only a real crash replays.
- **`auto.offset.reset=none` is the strict choice** for pipelines where silently starting from `latest`
  (skipping data) or `earliest` (reprocessing everything) would be a production incident. Fail, page a
  human, seek deliberately.
- **`offsetsForTimes` works on record timestamps** (`CreateTime` by default, set by the producer), which
  is what makes "replay the last hour" a one-liner, and what makes it wrong if producers have bad clocks.

## Key takeaways

- **Where the commit sits decides the guarantee.** Before handling is at-most-once (266 lost), after handling is
  at-least-once (234 processed twice); the second is Kafka's default contract.
- **Effectively-once needs an idempotent handler with durable memory.** Store the processed id in the same
  transaction as the side effect; an in-memory set would not survive the crash.
- **Auto-commit trails by up to 5 s and is safe only on the poll thread.** Hand records to another thread and it
  commits unfinished work: at-most-once by accident.

## When to use what

| Need | Do |
|---|---|
| simplest correct consumer | `enable.auto.commit=false`, process the batch, `commitSync()`; make the handler idempotent |
| high commit rate without blocking | `commitAsync()` per batch + `commitSync()` on shutdown / before rebalance (`onPartitionsRevoked`) |
| per-partition progress in a batch | `commitSync(Map<TopicPartition, OffsetAndMetadata>)` with `offset + 1` of the last processed record |
| replay a time window | `offsetsForTimes` + `seek`, or offline: `kafka-consumer-groups --reset-offsets --to-datetime` |
| skip a poison record | `seek(tp, record.offset() + 1)`, and write the record somewhere first |
| brand-new group must not skip history | `auto.offset.reset=earliest` |
| strictness over convenience | `auto.offset.reset=none` and an operational runbook for seeking |

---

← [07 · The poll loop and fetch tuning](07-consumer-fetch.md) · [Index](README.md) · [09 · Group protocol and rebalancing](09-consumer-rebalance.md) →
