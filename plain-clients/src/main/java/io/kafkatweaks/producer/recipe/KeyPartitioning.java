package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;

import java.time.Duration;
import java.util.Map;

/**
 * Chapter 04 · Partitioning: the partition decides ordering, batching and which consumer gets the load.
 * <p>
 * The real lever is the KEY, not a setting: same key, same partition ({@code murmur2(key) % partitions}), in order.
 * Key-less records go to the built-in sticky partitioner, which fills one partition's batch at a time and is why 4.x
 * batches well by default. Leave {@code partitioner.class} alone unless one of the cases below applies. Measured by the
 * {@code producer-partitioning} demo (docs/04-producer-partitioning.md), 3 000 key-less records/s over 6 partitions:
 * <pre>
 *   sticky (default)   batch-size-avg 11.2K
 *   roundRobin()       batch-size-avg  2847   (4x smaller batches for the same records)
 * </pre>
 */
public final class KeyPartitioning {

    private KeyPartitioning() {
    }

    /** Key-less records spread one by one over all partitions. Equal counts, at the price of 4x smaller batches. */
    public static Map<String, Object> roundRobin() {
        return Map.of(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
    }

    /**
     * Keep the key on the record (consumers still see it) but partition as if it were null: even spread and sticky
     * batching, and per-key ordering is GONE. For keys you need downstream but not for ordering.
     */
    public static Map<String, Object> ignoreKeys() {
        return Map.of(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, true);   // default false
    }

    /**
     * A routing policy of your own, e.g. {@link TenantPartitioner}. The producer instantiates the class (public no-arg
     * constructor) and calls it from every sending thread. Every producer of the topic must agree on it, forever.
     */
    public static Map<String, Object> partitioner(Class<? extends Partitioner> partitionerClass) {
        return Map.of(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass.getName());
    }

    /**
     * Stop sending key-less records to a partition whose leader has not accepted anything for {@code timeout}
     * (default 0 = off). On top of adaptive partitioning, which already favours fast leaders. Keyed records always go
     * to their partition.
     */
    public static Map<String, Object> skipUnresponsiveLeaders(Duration timeout) {
        return Map.of(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, timeout.toMillis());
    }
}
