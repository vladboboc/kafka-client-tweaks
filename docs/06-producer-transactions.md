# 06 · Transactions and exactly-once

**Demo:** `producer-transactions` · [ProducerTransactionsDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerTransactionsDemo.java)

## The problem

Idempotence (chapter 03) gives you "exactly one copy, in order" **per partition, per producer session**.
Two things it does not give you: atomicity across partitions or topics (write A and B, or neither), and
atomicity between *consuming* a record and *producing* its result. Both are what Kafka transactions add,
and both are what people mean by "exactly-once" in a Kafka-to-Kafka pipeline.

```mermaid
sequenceDiagram
    participant P as producer<br/>(transactional.id = X)
    participant TC as transaction coordinator
    participant T1 as partition out-0
    participant T2 as partition out-2
    participant OFF as __consumer_offsets
    P->>TC: initTransactions() — get PID + epoch, fence older X
    P->>TC: beginTransaction()
    P->>T1: records (marked with PID/epoch)
    P->>T2: records
    P->>TC: sendOffsetsToTransaction(offsets, groupMetadata)
    TC->>OFF: offsets, pending
    P->>TC: commitTransaction()
    TC->>T1: COMMIT marker
    TC->>T2: COMMIT marker
    TC->>OFF: COMMIT marker
    Note over T1,OFF: read_committed consumers see everything or nothing
```

## The knobs

| Config | Default | Meaning |
|---|---|---|
| `transactional.id` (producer) | none | enables transactions; **stable across restarts of the same logical instance**, that is what fencing keys on |
| `transaction.timeout.ms` (producer) | 60000 | a transaction open longer than this is aborted by the coordinator; must be ≤ broker `transaction.max.timeout.ms` (900000) |
| `enable.idempotence`, `acks=all`, `max.in.flight ≤ 5` | forced | transactions require them |
| `isolation.level` (consumer) | **`read_uncommitted`** | `read_committed` hides records of open and aborted transactions; opt in explicitly |
| `enable.auto.commit` (consumer, in a transactional processor) | `true` | must be `false`: offsets travel inside the transaction via `sendOffsetsToTransaction` |
| broker `transaction.state.log.replication.factor` / `.min.isr` | 3 / 2 | the transaction log is a topic like any other; this stack sets both |

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-transactions"
```

Arguments: `records=2000` (input records for part 3), `size=200`.

## What you should see

**1. Atomic writes.** Ten records over three partitions, aborted; ten more, committed. The two consumers
read the same topic from the beginning:

```
| isolation.level  | records seen | keys              |
| read_uncommitted |           13 | committed,aborted |
| read_committed   |           10 | committed         |
```

(`read_uncommitted` saw 13, not 20: the abort happened before the accumulator had flushed every batch,
so only 3 aborted records reached the log. It illustrates the point either way: aborted data is *in the
log*; `read_committed` skips it using the markers.)

**2. Commit cost.** Same 3 000 records, different transaction sizes:

```
| records per transaction     | records/s | commits/s |
|                           1 |     21.35 |     21.35 |
|                          10 |       247 |     24.74 |
|                         100 |      1896 |     18.96 |
|                        1000 |     12.5K |     12.45 |
| no transaction (idempotent) |     20.0K |         0 |
```

A commit is a handful of round trips (coordinator, then markers on every partition touched, replicated
with `acks=all`), ~40–50 ms on this stack. That is the whole story of the table: transactions are
practically free per record and ruinous per commit.

**3. Exactly-once consume-transform-produce.** 2 000 input records, a processor that upper-cases them into
an output topic and commits the input offsets *inside* each transaction. The first processor instance
"crashes" after 1 000 records in the middle of a transaction; a second instance with the **same
`transactional.id`** takes over:

```
processor-1 owns partitions [0, 1, 2]
processor-1 txn #1: p0[0..499] -> committed
processor-1 txn #2: p1[0..323] -> CRASH before commit (will be aborted)
processor-1 processed 1000 records and crashed mid-transaction (in-flight batch neither committed nor aborted)
processor-2 owns partitions [0, 1, 2]
processor-2 txn #1: p0[500..675] -> committed
processor-2 txn #2: p1[0..499] -> committed          <- the crashed batch, read again from the committed offset
processor-2 txn #3: p1[500..670] p2[0..328] -> committed
processor-2 txn #4: p2[329..652] -> committed
processor-2 (same transactional.id) resumed from the last committed offsets and processed 1500 records
processor-1's producer is a zombie now: InvalidProducerEpochException: Producer attempted to produce with an old epoch.

| output records (read_committed) | distinct input keys | keys seen twice | input records |
|                            2000 |                2000 |               0 |          2000 |
```

processor-1's second transaction had already sent part of its 324 records to the output topic when it
"crashed"; they are physically in the log, marked aborted, and `read_committed` never returns them.

## Reading the numbers

- **`initTransactions()` is the fence.** It bumps the producer epoch for that `transactional.id`, aborts
  whatever the previous incarnation left open, and turns the old producer into a zombie: its next commit
  fails with `ProducerFencedException`. This is why the id must be stable per logical instance (e.g.
  `<app>-<partition set>` or `<app>-<instance id>`), and never random per start.
- **Offsets are part of the transaction.** `sendOffsetsToTransaction(offsets, consumer.groupMetadata())`
  writes them to `__consumer_offsets` as *pending*; they become visible together with the output records
  on commit. A crash between "produce" and "commit offsets" cannot happen because there is no between.
- **`read_committed` on the input** matters when the input itself is produced transactionally, and on the
  output for anyone downstream. Downstream consumers using the default `read_uncommitted` see aborted
  records and open transactions: exactly-once ends at the first consumer that does not opt in.
- **Consumer lag looks different.** With `read_committed`, the consumer cannot read past the *last stable
  offset* (the first open transaction). A long-running transaction stalls every `read_committed` reader of
  that partition. Keep transactions short: per poll, or per 100 ms.
- **Not for everything.** Exactly-once is Kafka → Kafka. As soon as the side effect is an HTTP call or a
  database write, you are back to at-least-once plus an idempotent consumer (chapter 08).

## When to use what

| Situation | Setting |
|---|---|
| stream processor: read topic A, write topic B, no external side effects | transactional producer + `sendOffsetsToTransaction` + `isolation.level=read_committed` everywhere downstream |
| write an event to several topics atomically | one transactional producer, `beginTransaction` … `commitTransaction` around the sends |
| processor with database side effects | no Kafka transaction; the outbox/inbox pattern (see the sibling project) |
| batch size | commit per poll or on a timer (100–500 ms); never per record |
| many processor instances | one `transactional.id` per instance, derived from something stable (host, pod ordinal, assigned partition set) |
| transactions hanging around | lower `transaction.timeout.ms` so the coordinator aborts orphans sooner; watch broker metric `kafka.server:type=transaction-coordinator-metrics` |
