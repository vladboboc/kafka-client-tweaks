package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Chapter 04: which partition does a record land on, and why it matters for batching and ordering.
 * <ol>
 *   <li>null keys: the built-in sticky partitioner vs {@link RoundRobinPartitioner} (batch size)</li>
 *   <li>keyed records: hash spread, a hot key, and {@code partitioner.ignore.keys}</li>
 *   <li>a custom {@link Partitioner}</li>
 * </ol>
 * <pre>
 *   records=30000
 *   size=256
 *   rate=3000        records/s for the paced sticky vs round-robin comparison (8 s each)
 *   keys=20          distinct keys for the keyed runs
 *   hot=50           percentage of records that carry the single hot key
 * </pre>
 */
public final class ProducerPartitioningDemo implements Demo {

    private static final String TOPIC = "tweaks.partitioning";

    @Override
    public void run(Args args) {
        long records = args.getLong("records", 30_000);
        int size = args.getInt("size", 256);
        int keys = args.getInt("keys", 20);
        int hotPercent = args.getInt("hot", 50);

        try (var topics = new Topics()) {
            topics.ensure(TOPIC, 6);
        }
        // The demo's own properties, not a bare default producer: this table has to show what the runs below
        // actually use (linger.ms=20, any dotted override from the command line), with the "<- changed" markers.
        Knobs.printProducer(props(args, "knobs", Map.of()), ProducerConfig.PARTITIONER_CLASS_CONFIG,
                ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG,
                ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG,
                ProducerConfig.LINGER_MS_CONFIG, ProducerConfig.BATCH_SIZE_CONFIG);

        // ---- 1. null keys: sticky (default) vs round robin -------------------------------------
        int rate = args.getInt("rate", 3000);
        long pacedRecords = Math.min(records, (long) rate * 8);
        Workload.warmUp(TOPIC);
        System.out.printf("%n1. null keys at a steady %d records/s (so batches close on linger.ms, not on batch.size):%n", rate);
        System.out.println("   the default sticky partitioner fills ONE partition's batch until linger expires, then moves to another;");
        System.out.println("   RoundRobinPartitioner spreads consecutive records over all partitions, so each batch gets 1/6 of the records.\n");
        var sticky = Workload.run("sticky (default)", props(args, "sticky", Map.of()), TOPIC, pacedRecords, size, Workload.Payload.JSON, 0, rate);
        var roundRobin = Workload.run("round-robin", props(args, "rr",
                Map.of(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName())), TOPIC, pacedRecords, size, Workload.Payload.JSON, 0, rate);
        Workload.printComparison(List.of(sticky, roundRobin));
        Workload.printPartitionSpread(sticky);
        System.out.println("   (records/s is the pacing rate for both; look at batch-size-avg, records-per-request-avg and request-rate)");

        // ---- 2. keyed records --------------------------------------------------------------------
        System.out.printf("%n2. keyed records: partition = murmur2(key) %% partitions. Same key, same partition, always (unless the%n"
                + "   partition count changes). %d keys over 6 partitions is not uniform, and a hot key is a hot partition.%n", keys);
        spread("hash of %d keys".formatted(keys), props(args, "keyed", Map.of()), records, size, i -> Payloads.key(i, keys));
        spread("hot key: %d%% of records share one key".formatted(hotPercent), props(args, "hot", Map.of()), records, size,
                i -> (i % 100) < hotPercent ? "customer-hot" : Payloads.key(i, keys));
        spread("same keys, partitioner.ignore.keys=true (ordering per key is GONE)",
                props(args, "ignore-keys", Map.of(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, "true")), records, size,
                i -> Payloads.key(i, keys));

        // ---- 3. custom partitioner --------------------------------------------------------------
        System.out.println("\n3. a custom Partitioner: keys starting with \"vip-\" go to partition 0, everything else is hashed over 1..N-1.");
        spread("TenantPartitioner", props(args, "custom", Map.of(ProducerConfig.PARTITIONER_CLASS_CONFIG, TenantPartitioner.class.getName())),
                records, size, i -> (i % 10 == 0) ? "vip-" + (i % 3) : Payloads.key(i, keys));

        System.out.println("""

                adaptive partitioning (partitioner.adaptive.partitioning.enable=true, default): for null-key records the
                sticky partitioner prefers partitions whose leader is answering fast; a slow broker gets fewer records.
                partitioner.availability.timeout.ms=N (default 0 = off) goes further and stops sending to a partition
                whose leader has not accepted anything for N ms. Neither applies to keyed records.
                """);
    }

    private interface KeyFn {
        String key(long i);
    }

    private static void spread(String label, Properties props, long records, int size, KeyFn keyFn) {
        var perPartition = new ConcurrentHashMap<Integer, Integer>();
        var perKeyPartition = new ConcurrentHashMap<String, Integer>();
        var keyMovedPartition = new ConcurrentHashMap<String, Boolean>();
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (long i = 0; i < records; i++) {
                String key = keyFn.key(i);
                producer.send(new ProducerRecord<>(TOPIC, key, Payloads.json(i, size)), (RecordMetadata md, Exception ex) -> {
                    if (ex != null) {
                        return;
                    }
                    perPartition.merge(md.partition(), 1, Integer::sum);
                    Integer previous = perKeyPartition.putIfAbsent(key, md.partition());
                    if (previous != null && previous != md.partition()) {
                        keyMovedPartition.put(key, Boolean.TRUE);
                    }
                });
            }
            producer.flush();
        }
        var table = new Table("partition", "records", "share");
        new TreeMap<>(perPartition).forEach((p, n) -> table.row(p, n, "%.1f%%".formatted(100d * n / records)));
        table.print(label);
        System.out.printf("   distinct keys: %d, keys that landed on more than one partition: %d%n",
                perKeyPartition.size(), keyMovedPartition.size());
    }

    private static Properties props(Args args, String clientId, Map<String, String> overrides) {
        var p = Env.producer("partitioning-" + clientId);
        p.put(ProducerConfig.LINGER_MS_CONFIG, "20");   // give batches time to fill so batch-size-avg is meaningful
        p.putAll(overrides);
        return args.applyOverrides(p);
    }

    /**
     * Example custom partitioner: a small set of "vip" tenants gets a dedicated partition (and therefore a
     * dedicated consumer, if the group has as many members as partitions); everyone else is hashed over
     * the remaining partitions with the same murmur2 hash the default partitioner uses.
     */
    public static final class TenantPartitioner implements Partitioner {

        // Partitioners are shared by all sending threads: state must be thread-safe.
        private final java.util.concurrent.atomic.AtomicInteger roundRobin = new java.util.concurrent.atomic.AtomicInteger();

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

}
