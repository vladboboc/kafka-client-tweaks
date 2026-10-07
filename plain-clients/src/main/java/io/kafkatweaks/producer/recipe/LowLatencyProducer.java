package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

/**
 * Chapter 05 · Latency first: send each record as soon as the sender thread is free, and stay durable.
 * <p>
 * At low and moderate rates batches never fill, so every record waits the full {@code linger.ms}: it is a latency
 * floor. Measured by the {@code producer-low-latency} demo (docs/05-producer-low-latency.md), sequential
 * {@code send().get()}, p50 ack latency:
 * <pre>
 *   throughput-tuned (linger 50, zstd)   55.45 ms
 *   client defaults (linger 5)            9.57 ms
 *   lowLatency()                          2.80 ms   (still acks=all + idempotence)
 *   lowLatencyLeaderAck()                 2.04 ms   (acks=1: chapter 03's risk for 0.8 ms)
 * </pre>
 * Leave {@code batch.size} at its default (a maximum, not a minimum) and {@code compression.type} at {@code none}:
 * small batches gain nothing from compression, and the call costs CPU before the request leaves.
 */
public final class LowLatencyProducer {

    private LowLatencyProducer() {
    }

    /** No waiting for company. acks=all and idempotence stay on: they cost well under a millisecond here. */
    public static Map<String, Object> lowLatency() {
        return Map.of(ProducerConfig.LINGER_MS_CONFIG, 0);   // default 5 ms (4.x)
    }

    /** Also drop the follower round trip from the ack. Only for data you can rebuild: see {@link DurableProducer#leaderOnly()}. */
    public static Map<String, Object> lowLatencyLeaderAck() {
        return Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);   // idempotence requires acks=all
    }

    /**
     * Fetch the topics' metadata before the first real record, so that record does not pay for it. Call it right after
     * creating the producer, e.g. at application startup. Chapter 01's demo times two synchronous sends: the first is
     * several times slower than the second (~15 ms), and the metadata round trip is part of that difference.
     */
    public static void warmUp(Producer<?, ?> producer, String... topics) {
        for (String topic : topics) {
            producer.partitionsFor(topic);
        }
    }
}
