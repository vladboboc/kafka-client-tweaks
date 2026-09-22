# 05 · Latency first

**Demo:** `producer-low-latency` · [ProducerLowLatencyDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerLowLatencyDemo.java) · **Recipe:** [LowLatencyProducer.java](../plain-clients/src/main/java/io/kafkatweaks/producer/recipe/LowLatencyProducer.java)

## The problem

Chapter 02 bought throughput with `linger.ms`, big batches and compression, and paid with latency:
every record waited for its batch. Some workloads care about the opposite: a command that must be
acknowledged before an HTTP response goes out, a change event that a downstream service is waiting for,
a heartbeat. For those the question is: what is the shortest path from `send()` to the callback, and what
does each safety feature add to it?

## The knobs

| Config | Latency-first value | Why |
|---|---|---|
| `linger.ms` | `0` | send a batch as soon as the sender thread can; never wait for company |
| `batch.size` | leave the default (16 KB) | it is a maximum, not a minimum; with `linger.ms=0` batches close as soon as the sender is free |
| `compression.type` | `none` | compression is per batch and costs CPU before the request leaves; small batches gain nothing from it |
| `acks` | `all` unless you can afford chapter 03's risk | `acks=1` removes the follower round trip from the ack, `acks=0` removes the ack itself |
| `enable.idempotence` | `true` | free in latency terms; keep it |
| `max.in.flight.requests.per.connection` | 5 (default) | more in-flight requests do not lower latency; fewer make a burst queue |
| `send()` style | asynchronous with a callback | `send().get()` per record serialises the round trips |

The floor is `request-latency-avg`: the broker round trip including replication, which no producer
setting can remove. On this Docker stack it is 2–10 ms.

## The code that matters

One setting, and a warm-up. From [LowLatencyProducer.java](../plain-clients/src/main/java/io/kafkatweaks/producer/recipe/LowLatencyProducer.java):

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/producer/recipe/LowLatencyProducer.java -->
```java
public static Map<String, Object> lowLatency() {
    return Map.of(ProducerConfig.LINGER_MS_CONFIG, 0);   // default 5 ms (4.x)
}
// ...
public static void warmUp(Producer<?, ?> producer, String... topics) {
    for (String topic : topics) {
        producer.partitionsFor(topic);
    }
}
```

- **`linger.ms=0` took p50 from 9.57 ms (defaults) to 2.80 ms** for sequential sends, with `acks=all` and
  idempotence still on. At low rates a batch never fills, so every record waits the full `linger.ms`: 55 ms with
  chapter 02's throughput settings.
- `lowLatencyLeaderAck()` (acks=1) bought another 0.8 ms; that is chapter 03's durability price, only for data you
  can rebuild.
- **Leave `batch.size` and `compression.type` alone**: a batch is sent as soon as the sender is free, and compressing
  a batch of a few hundred bytes costs CPU for nothing.
- `warmUp(producer, topic)` at startup keeps the metadata fetch out of the first real record's latency.

The demo's `PRESETS` in [ProducerLowLatencyDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerLowLatencyDemo.java)
go from chapter 02's throughput settings to the two recipe presets; everything else in that file is measurement.

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-low-latency"
```

Arguments: `sync-records=300`, `rate=500`, `seconds=10`, `size=256`.

## What you should see

**1. Sequential `send().get()`** (each record waits for its ack before the next is sent):

```
| preset                             | p50 ms | p99 ms | max ms |
|------------------------------------|--------|--------|--------|
| throughput-tuned (linger=50, zstd) |  55.45 |  82.10 |    151 |
| defaults (linger=5)                |   9.57 |  16.60 |  28.78 |
| linger=0                           |   2.80 |   7.41 |  12.00 |
| linger=0, acks=1                   |   2.04 |   5.85 |  14.52 |
```

**2. A paced asynchronous stream** at 500 records/s, each record timed from `send()` to callback:

```
| preset                             | p50 ms | p99 ms | batch-size-avg | records/request | request-latency-avg | queue-time-avg |
|------------------------------------|--------|--------|----------------|-----------------|---------------------|----------------|
| throughput-tuned (linger=50, zstd) |  31.70 |  56.08 |            501 |            9.55 |                3.71 |          50.15 |
| defaults (linger=5)                |   8.88 |    115 |            463 |            1.50 |                7.21 |           5.38 |
| linger=0                           |   4.68 |  84.70 |            349 |            1.07 |                9.65 |           0.48 |
| linger=0, acks=1                   |   2.36 |  86.87 |            340 |            1.04 |                5.05 |           0.24 |
```

## Reading the numbers

- **`linger.ms` is a latency floor whenever batches are not filling on their own.** At 500 records/s a
  16 KB batch never fills, so with `linger.ms=50` every record waits the full 50 ms
  (`record-queue-time-avg` = 50.15) and gets acknowledged at ~55 ms. With the 4.x default of 5 ms the
  floor is 5 ms; with `linger.ms=0` it is gone (0.48 ms of queue time, which is just the sender thread's
  reaction time).
- **The sequential table is the honest view of round-trip cost**: ~2.8 ms with `linger.ms=0`, of which
  `acks=all` replication is about 0.8 ms on this stack (compare the last two rows). In production the
  replication hop is a network round trip between brokers; measure it before deciding it is worth
  dropping to `acks=1`.
- **Compression at low rates is pure overhead**: `records/request` is 9.5 in the first row only because
  the batch waited 50 ms; the 501 bytes it produced cost a zstd call and gained nothing.
- **p99 in the paced run is noisy** (85–115 ms across presets): that is GC, Docker networking and the
  Windows scheduler, not Kafka configuration. Tail latency needs JVM tuning and a quiet host; producer
  settings decide p50.
- **Idempotence stays on.** Compare rows 3 and 4: the difference is `acks`, not idempotence.

## When to use what

| Situation | Setting |
|---|---|
| request/response style, one record at a time, waiting for the ack | `linger.ms=0`, `compression.type=none`, keep `acks=all` + idempotence |
| low-rate stream where every record matters | same; `acks=1` only if the topic's data is reconstructible |
| moderate rate, latency budget of a few ms | `linger.ms=1–2` gives small batches without a visible floor |
| moderate rate, latency budget of 20 ms+ | go back to chapter 02: `linger.ms=10–20` and compression pay for themselves |
| mixed traffic in one application | two producers with two configurations; a producer is cheap, a wrong `linger.ms` is not |
