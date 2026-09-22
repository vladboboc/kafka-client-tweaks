package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Chapter 02 · Throughput: send fewer, bigger, compressed produce requests.
 * <p>
 * A producer is limited by {@code brokers x max.in.flight x bytes-per-request / request-latency}; every knob here
 * raises the bytes per request. Measured by the {@code producer-batching} demo (docs/02-producer-batching-compression.md),
 * 30 000 JSON records of 512 bytes, 3 brokers, {@code acks=all}:
 * <pre>
 *   client defaults                    7.4K records/s   ack p50  2398 ms     27 records per request
 *   batching(20, 64 * 1024, "zstd")   35.6K records/s   ack p50   188 ms    169 records per request
 *   highThroughput()                 162.8K records/s   ack p50    49 ms   7500 records per request
 * </pre>
 * Usage: {@code props.putAll(ThroughputProducer.highThroughput());} before {@code new KafkaProducer<>(props)}.
 */
public final class ThroughputProducer {

    private ThroughputProducer() {
    }

    /** The chapter's best run for a JSON firehose that can afford ~50 ms of latency: 100 ms linger, 256 KB batches, zstd. */
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

    /**
     * Back-pressure. {@code buffer.memory} is the whole accumulator; when it is full, {@code send()} blocks for at most
     * {@code max.block.ms} and then REJECTS the record through its callback and future. It does not throw, so count the
     * failures with {@link FailureCounter}. A bigger buffer buys more buffering (and latency), never more throughput.
     */
    public static Map<String, Object> bounded(long bufferMemoryBytes, long maxBlockMs) {
        return Map.of(
                ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemoryBytes,   // default 32 MB, shared by all partitions
                ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);          // default 60 s, also covers waiting for metadata
    }

    /**
     * A {@link Callback} that counts failed sends. Since Kafka 3.x a full accumulator ({@code BufferExhaustedException}
     * after {@code max.block.ms}) completes the record's future exceptionally and calls this callback; {@code send()}
     * itself returns normally. A producer that passes no callback and never reads the future drops those records
     * without noticing. Pass one instance to every {@code send(record, counter)}; it is thread-safe.
     */
    public static final class FailureCounter implements Callback {

        private final AtomicLong failed = new AtomicLong();
        private final AtomicReference<Exception> first = new AtomicReference<>();

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                failed.incrementAndGet();
                first.compareAndSet(null, exception);
            }
        }

        public long failed() {
            return failed.get();
        }

        /** The first failure, or null: usually enough to tell a full buffer from a broker problem. */
        public Exception firstFailure() {
            return first.get();
        }
    }
}
