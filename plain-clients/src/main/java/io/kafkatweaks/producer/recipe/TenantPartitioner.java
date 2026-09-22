package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Chapter 04 · A custom {@link Partitioner}: a small set of "vip" tenants gets a dedicated partition (and therefore a
 * dedicated consumer, if the group has as many members as partitions); everyone else is hashed over the remaining
 * partitions with the same murmur2 hash the default partitioner uses.
 * <p>
 * Register it with {@code props.putAll(KeyPartitioning.partitioner(TenantPartitioner.class))}.
 */
public final class TenantPartitioner implements Partitioner {

    // Partitioners are shared by all sending threads: state must be thread-safe.
    private final AtomicInteger roundRobin = new AtomicInteger();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // partitionCountForTopic is declared Integer and is null for a topic this Cluster snapshot does not
        // know; a Partitioner must still return a partition that exists, so treat it as the single partition 0.
        Integer count = cluster.partitionCountForTopic(topic);
        int partitions = count == null ? 1 : count;
        if (partitions <= 1) {
            return 0;   // nothing to reserve for vips: everything goes to the only partition there is
        }
        if (key instanceof String s && s.startsWith("vip-")) {
            return 0;
        }
        if (keyBytes == null) {
            return 1 + Math.floorMod(roundRobin.getAndIncrement(), partitions - 1);   // no key: round-robin over the non-vip partitions
        }
        return 1 + Utils.toPositive(Utils.murmur2(keyBytes)) % (partitions - 1);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
