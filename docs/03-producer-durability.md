# 03 · Durability, ordering and retries

**Demo:** `producer-durability` · [ProducerDurabilityDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerDurabilityDemo.java)

## The problem

"The send succeeded" can mean five different things. This chapter is about choosing which one, and
about what the producer does between a failed request and the callback.

## The knobs

| Config | Default | Meaning |
|---|---|---|
| `acks` | `all` | how many replicas must have the record before the broker answers: `0` none (fire and forget), `1` the leader, `all` every in-sync replica |
| `min.insync.replicas` (**topic/broker**) | 1 (this cluster: 2) | the floor for `acks=all`: fewer in-sync replicas than this and the write is refused with `NotEnoughReplicasException` |
| `enable.idempotence` | `true` | producer id + per-partition sequence numbers; retries cannot duplicate or reorder. Requires `acks=all`, `max.in.flight ≤ 5`, `retries > 0`; the 4.x client **refuses** to start with `acks=0/1` while it is on |
| `max.in.flight.requests.per.connection` | 5 | requests in flight per broker; with idempotence, ordering is kept up to 5 |
| `retries` | `MAX_INT` | practically unlimited; the real limit is `delivery.timeout.ms` |
| `retry.backoff.ms` / `retry.backoff.max.ms` | 100 / 1000 | exponential backoff between retries |
| `request.timeout.ms` | 30000 | one request's budget |
| `delivery.timeout.ms` | 120000 | total budget from `send()` to callback, retries included. Must be ≥ `linger.ms + request.timeout.ms` |
| `max.block.ms` | 60000 | how long `send()` itself may block (metadata, buffer) |

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-durability"
```

Part 3 stops a broker. Either follow the on-screen instructions (`docker compose stop kafka-N`, later
`start`), or let the demo do it: `broker-control=docker`. Arguments: `records=20000`, `size=512`,
`wait=90`, `skip-acks=true`, `skip-isr=true`.

## What you should see

**1. acks on the same workload.** On a healthy local cluster the difference is small and noisy:

```
| run                              | records/s | MB/s  | ack p50 ms | ack p99 ms | errors |
| acks=0                           |     15.3K |  8.29 |        194 |        340 |      0 |
| acks=1                           |     18.6K | 10.11 |        268 |        388 |      0 |
| acks=all, no idempotence         |     18.0K |  9.75 |        279 |        437 |      0 |
| acks=all + idempotence (default) |     13.3K |  7.19 |        309 |        590 |      0 |
```

`acks=0` shows `request-latency-avg = NaN`: it never sees a response. The point of the table is what
it does **not** show: none of the runs lost anything, because nothing failed. `acks` is about what
happens when something does.

**2. The timeout chain.** A producer configured with `delivery.timeout.ms=5000` and
`request.timeout.ms=30000` is rejected at construction:

```
ConfigException: delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms
```

**3. `min.insync.replicas` in action** on a topic with RF=2, `min.insync.replicas=2`
(`tweaks.durability-rf2`), stopping the follower:

```
[before     ] acks=all -> OK   offset 0 after 731 ms
$ docker stop kafka-1
| partition | leader   | replicas | in-sync replicas |
|         0 | broker 2 |      1,2 |                2 |
WARN Sender - Got error produce response ... retrying (2147483646 attempts left). Error: NOT_ENOUGH_REPLICAS
WARN Sender - Got error produce response ... retrying (2147483645 attempts left). Error: NOT_ENOUGH_REPLICAS
...
[broker down] acks=all -> FAIL after 8102 ms: TimeoutException: Expiring 1 record(s) for tweaks.durability-rf2-0:8001 ms has passed since batch creation.
[broker down] acks=1   -> OK   offset 1 after 138 ms
              ^ the leader accepted it alone; if that broker dies now the record is gone
$ docker start kafka-1
[recovered  ] acks=all -> OK   offset 2 after 66 ms
```

(The demo shortens `delivery.timeout.ms` to 8 s so the failure shows up in seconds instead of two minutes.)

## Reading the numbers

- **`NOT_ENOUGH_REPLICAS` is retriable.** The producer retried with exponential backoff until
  `delivery.timeout.ms` ran out and only then failed the record. In production, with the default 120 s,
  a short ISR shrink is invisible to the application. That is the design: durability first, latency second.
- **`acks=1` kept "working"** with one replica alive. Offset 1 exists on one disk. If broker 2 dies before
  broker 1 catches up, offset 1 is gone and the caller was told it was written.
- **The cluster itself never blinked**: the KRaft quorum needs 2 of 3 controllers, and every other topic
  here has RF=3 with `min.insync.replicas=2`, so they kept accepting `acks=all` writes with one broker down.
- **Idempotence costs nothing you can see** (row 4 vs row 3 is noise), and it is the only thing that makes
  `retries` safe. Before it, a retried batch could land twice or out of order. Leave it on.
- **`retries` is not the knob to turn.** It is effectively infinite; `delivery.timeout.ms` is the budget
  you configure, and it should be as large as the longest outage you want to ride through invisibly.

## When to use what

| Need | Setting |
|---|---|
| must not lose acknowledged records | `acks=all`, topic `min.insync.replicas=2`, RF=3, idempotence on (all defaults except `min.insync.replicas`) |
| can tolerate loss on broker failure, want lower latency | `acks=1`, `enable.idempotence=false` |
| metrics/logs where a gap is acceptable and the ack round trip is not | `acks=0`, `enable.idempotence=false` |
| ride through a broker restart without errors | keep `delivery.timeout.ms` at 120 s or higher; alert on `record-retry-total` instead |
| fail fast, let the caller decide | `delivery.timeout.ms` short (≥ `linger.ms + request.timeout.ms`), handle the callback's exception |
| strict ordering per partition | idempotence on (default). Without it, `max.in.flight.requests.per.connection=1` is the only way, and it halves throughput |
