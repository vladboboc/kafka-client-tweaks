# 02 · Throughput: batching, compression and the accumulator

**Demo:** `producer-batching` · [ProducerBatchingDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerBatchingDemo.java) · **Recipe:** [ThroughputProducer.java](../plain-clients/src/main/java/io/kafkatweaks/producer/recipe/ThroughputProducer.java)

## The problem

Chapter 01 ended at ~4 MB/s with 1.6 s of queueing latency, because the producer was limited by

```
throughput ≈ brokers × max.in.flight × bytes-per-request / request-latency
```

Every knob in this chapter attacks *bytes per request*: put more records into each batch, and make each
record smaller on the wire.

## The knobs

| Config | Default | What it does | Trade-off |
|---|---|---|---|
| `batch.size` | 16384 | max bytes of **uncompressed** records in one batch (per partition). A batch is sent when full, or when `linger.ms` expires | too big + low traffic = batches never fill and always wait `linger.ms` |
| `linger.ms` | **5** (4.x; was 0) | how long a non-full batch waits for more records | every record pays up to this in latency |
| `compression.type` | `none` | `lz4`, `snappy`, `zstd`, `gzip`; compresses the whole batch, so it works better with bigger batches | CPU on producer *and* on every consumer; nothing on the broker (it stores batches as received) |
| `compression.zstd.level` / `.gzip.level` / `.lz4.level` | 3 / 6 / 9 | codec level | more CPU for a few % smaller |
| `buffer.memory` | 32 MB | total accumulator; when full, `send()` blocks | more memory = more buffering, **not** more throughput |
| `max.block.ms` | 60000 | how long `send()` may block for buffer space or metadata before throwing | |
| `max.request.size` | 1 MB | hard cap on one request (also caps a single record) | broker's `message.max.bytes` must agree |

## The code that matters

Three settings, from [ThroughputProducer.java](../plain-clients/src/main/java/io/kafkatweaks/producer/recipe/ThroughputProducer.java);
`props.putAll(ThroughputProducer.highThroughput())` before `new KafkaProducer<>(props)` is the whole change:

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/producer/recipe/ThroughputProducer.java -->
```java
public static Map<String, Object> highThroughput() {
    return batching(100, 256 * 1024, "zstd");
}

public static Map<String, Object> batching(int lingerMs, int batchSizeBytes, String compressionType) {
    return Map.of(
            // How long a batch that is not full waits for more records. Default 5 ms (0 before Kafka 4.0).
            // It only matters at LOW traffic: under a firehose batches fill up long before it expires.
            ProducerConfig.LINGER_MS_CONFIG, lingerMs,
            // Max bytes of UNCOMPRESSED records per partition batch. Default 16 KB. This is the lever under load:
            // 16 KB -> 64 KB more than doubled throughput. It is per partition: buffer.memory must hold
            // partitions x batch.size.
            ProducerConfig.BATCH_SIZE_CONFIG, batchSizeBytes,
            // none | lz4 | snappy | zstd | gzip. Default none. Compresses whole batches, so bigger batches compress
            // better: JSON went out at 6% of its size. zstd has the best ratio at a CPU cost close to lz4. Costs CPU
            // on the producer and on every consumer, nothing on the broker (it stores the batch as received).
            ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
}
```

- **`batch.size` moves the numbers, `linger.ms` alone does not**: rows 1–3 below are flat, 16 KB → 64 KB more than
  doubles throughput (7.6K → 17.1K records/s).
- **`zstd` is the big win for JSON**: `compression-rate-avg` 0.06, 35.6K records/s at the same batching.
- **`highThroughput()`** (100 ms / 256 KB / zstd) is the last row: 162.8K records/s, 7 500 records per request.
- **Back-pressure** is `ThroughputProducer.bounded(bufferMemory, maxBlockMs)` plus a callback that counts
  failures (`ThroughputProducer.FailureCounter`): a full buffer fails the record's future and callback, `send()`
  does not throw.

The demo's `PRESETS` map in [ProducerBatchingDemo.java](../plain-clients/src/main/java/io/kafkatweaks/producer/ProducerBatchingDemo.java)
builds the codec rows and the last row from the recipe; the first rows change one knob at a time to show which one
matters. Everything else in that file is measurement.

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-batching"
```

Arguments: `records=30000`, `size=512`, `payload=json|random`, `runs=linger0,defaults,zstd` (any subset of the
presets in the source), `buffer-demo=true`, `buffer-records=60000`.

Try `payload=random`: incompressible data turns every compression preset into pure CPU cost.

## What you should see

Presets run one after another on the same topic and the same 30 000 × 512-byte JSON records. One run of this
stack (Docker Desktop, 3 brokers, `acks=all`, `min.insync.replicas=2`):

```
| run                      | records/s | MB/s  | ack p50 ms | ack p99 ms |
|--------------------------|-----------|-------|------------|------------|
| linger0                  |      7875 |  4.31 |       2097 |       2932 |
| defaults                 |      7432 |  4.07 |       2398 |       3487 |
| linger20                 |      7557 |  4.13 |       1482 |       3606 |
| linger20-batch64k        |     17.1K |  9.36 |        829 |       1487 |
| lz4                      |     18.1K |  9.89 |        665 |        755 |
| snappy                   |     18.2K |  9.97 |        616 |        828 |
| zstd                     |     35.6K | 19.49 |        188 |        279 |
| zstd-level9              |     56.0K | 30.62 |      21.39 |      34.61 |
| gzip                     |     50.3K | 27.52 |      13.94 |      47.85 |
| linger100-batch256k-zstd |    162.8K | 89.04 |      49.09 |      77.68 |
```

and the metrics that explain it:

```
| run                      | batch-size-avg | records-per-request-avg | compression-rate-avg | request-latency-avg | record-queue-time-avg |
| linger0                  |          15.9K |                   27.25 |                    1 |               49.75 |                  1821 |
| linger20-batch64k        |          65.1K |                     112 |                    1 |               78.22 |                   758 |
| zstd                     |           6221 |                     169 |                 0.06 |               58.20 |                   120 |
| linger100-batch256k-zstd |         162.6K |                    7500 |                 0.04 |               21.50 |                    42 |
```

Then the back-pressure table: the same bounded workload with a 32 MB buffer (everything sent,
`bufferpool-wait-ratio` ≈ 0) and with a 1 MB buffer plus `max.block.ms=20`, where the wait ratio climbs and
records start being **rejected** (right at the edge on this stack: 0, 1 and 4 of 60 000 in three runs; run it
twice before concluding nothing happens). Note how that failure arrives: `send()` does not throw. When `max.block.ms`
expires the record's future is completed with a `BufferExhaustedException` (a `TimeoutException`) and the
callback is invoked with it, so the `outcome` column here comes from counting callback errors. A producer that
passes no callback and never looks at the returned future loses those records without any sign of it.

## Reading the numbers

- **`linger.ms` alone did nothing** here (rows 1–3): the accumulator was already full, batches were closing
  because they hit `batch.size`, not because linger expired. Linger matters when traffic is *low*; under a
  firehose `batch.size` is the lever.
- **`batch.size` 16 K → 64 K** doubled throughput: 4× the records per request, at slightly higher request
  latency (bigger payloads).
- **Compression** is the big win for JSON: `compression-rate-avg` 0.06 means a 64 KB batch went out as
  ~4 KB. Fewer bytes on the wire, fewer bytes replicated, fewer bytes on disk, and fewer bytes for every
  consumer to fetch. `batch-size-avg` reports the *compressed* size, which is why it shrinks.
- **zstd** is the usual default choice: best ratio, CPU cost close to lz4. `gzip` compresses as well but
  costs the most CPU; `lz4`/`snappy` are the fast-and-light options when producer CPU is the constraint.
- **The last row** shows what "throughput tuning" really means: 256 KB batches, 100 ms linger, zstd.
  7 500 records per request, 20× the baseline, at 50 ms p50 latency instead of 2 s. That latency is the
  price, and chapter 05 is about not paying it.
- **Presets run sequentially**, so later presets benefit from a warm JVM and page cache. Reorder with
  `runs=` or run the demo twice before drawing fine-grained conclusions; the differences between the
  groups above are far larger than that noise.

## When to use what

| Situation | Setting |
|---|---|
| high-volume event stream, latency in the tens of ms is fine | `linger.ms=20–100`, `batch.size=128–512 KB`, `compression.type=zstd` |
| text/JSON/Avro payloads | always compress; `zstd` (or `lz4` if producer CPU is scarce) |
| already-compressed payloads (images, encrypted blobs) | `compression.type=none`, save the CPU |
| many partitions per producer | remember `batch.size` is *per partition*; `buffer.memory` must hold `partitions × batch.size` at least |
| `send()` blocks or throws `TimeoutException` on buffer | you are producing faster than the cluster accepts; bigger `buffer.memory` only delays the problem. Compress, batch bigger, add partitions/brokers, or shed load |
