package io.kafkatweaks.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class TenantPartitionerTest {

    private static final String TOPIC = "t";
    private static final Cluster CLUSTER = cluster(6);
    private final ProducerPartitioningDemo.TenantPartitioner partitioner = new ProducerPartitioningDemo.TenantPartitioner();

    @Test
    void vipKeysAlwaysGoToPartitionZero() {
        for (String key : List.of("vip-1", "vip-acme", "vip-")) {
            assertThat(partition(key)).isZero();
        }
    }

    @Test
    void otherKeysNeverUsePartitionZeroAndAreDeterministic() {
        for (int i = 0; i < 200; i++) {
            String key = "customer-" + i;
            int p = partition(key);
            assertThat(p).isBetween(1, 5);
            assertThat(partition(key)).isEqualTo(p);
        }
    }

    @Test
    void nullKeysAreSprayedOverNonVipPartitions() {
        Set<Integer> seen = new java.util.HashSet<>();
        for (int i = 0; i < 500; i++) {
            int p = partitioner.partition(TOPIC, null, null, "v", "v".getBytes(StandardCharsets.UTF_8), CLUSTER);
            assertThat(p).isBetween(1, 5);
            seen.add(p);
        }
        assertThat(seen).hasSizeGreaterThan(1);
    }

    /** A Partitioner must return a partition that exists for every topic it is handed, one-partition ones included. */
    @Test
    void aSinglePartitionTopicLeavesNothingToReserve() {
        Cluster one = cluster(1);
        byte[] key = "customer-1".getBytes(StandardCharsets.UTF_8);
        assertThat(partitioner.partition(TOPIC, "customer-1", key, "v", null, one)).isZero();
        assertThat(partitioner.partition(TOPIC, "vip-1", "vip-1".getBytes(StandardCharsets.UTF_8), "v", null, one)).isZero();
        assertThat(partitioner.partition(TOPIC, null, null, "v", null, one)).isZero();
    }

    private int partition(String key) {
        return partitioner.partition(TOPIC, key, key.getBytes(StandardCharsets.UTF_8), "v", "v".getBytes(StandardCharsets.UTF_8), CLUSTER);
    }

    private static Cluster cluster(int partitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> infos = IntStream.range(0, partitions)
                .mapToObj(p -> new PartitionInfo(TOPIC, p, node, new Node[] {node}, new Node[] {node}))
                .toList();
        return new Cluster("test", List.of(node), infos, Set.of(), Set.of());
    }
}
