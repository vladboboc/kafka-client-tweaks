package io.kafkatweaks;

import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Single entry point for every demo, so each chapter can be run with the same command shape:
 * <pre>
 *   ./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-batching records=20000 compression=zstd"
 * </pre>
 * Run without arguments to list the demos. Kafka client properties can be passed as extra
 * {@code key=value} pairs; every key containing a dot is applied on top of the demo's own configuration.
 */
public final class Run {

    private static final Logger log = LoggerFactory.getLogger(Run.class);

    private record Entry(String chapter, String summary, Demo demo) {
    }

    private static final Map<String, Entry> DEMOS = new LinkedHashMap<>();

    static {
        // One line per chapter, in the order of the docs.
        register("producer-baseline", "01", "untuned producer: send path, sync vs async send, default knobs, reading metrics",
                new io.kafkatweaks.producer.ProducerBaselineDemo());
        register("producer-batching", "02", "throughput: batch.size, linger.ms, compression.type/levels, buffer.memory, max.block.ms",
                new io.kafkatweaks.producer.ProducerBatchingDemo());
        register("producer-durability", "03", "acks 0/1/all, idempotence, the timeout chain, min.insync.replicas with a broker stopped",
                new io.kafkatweaks.producer.ProducerDurabilityDemo());
        register("producer-partitioning", "04", "sticky vs round-robin, keyed hashing, hot keys, partitioner.ignore.keys, custom Partitioner",
                new io.kafkatweaks.producer.ProducerPartitioningDemo());
        register("producer-low-latency", "05", "latency first: linger.ms=0, acks, compression off; sequential and paced workloads",
                new io.kafkatweaks.producer.ProducerLowLatencyDemo());
        register("producer-transactions", "06", "atomic writes, read_committed, commit cost, exactly-once consume-transform-produce, zombie fencing",
                new io.kafkatweaks.producer.ProducerTransactionsDemo());
        register("consumer-fetch", "07", "poll loop: max.poll.records, fetch.min/max.bytes, fetch.max.wait.ms, max.poll.interval.ms and a slow handler",
                new io.kafkatweaks.consumer.ConsumerFetchDemo());
        register("consumer-offsets", "08", "commit strategies: at-most-once, at-least-once, idempotent handler, auto-commit timing, auto.offset.reset, seek",
                new io.kafkatweaks.consumer.ConsumerOffsetsDemo());
        register("consumer-rebalance", "09", "group.protocol consumer (KIP-848) vs classic, assignors, static membership; a live rebalance timeline",
                new io.kafkatweaks.consumer.ConsumerRebalanceDemo());
        register("consumer-parallel", "10", "scaling: partitions vs consumers, per-partition workers on virtual threads, pause/resume back-pressure",
                new io.kafkatweaks.consumer.ConsumerParallelDemo());
        register("consumer-share", "11", "Queues for Kafka (KIP-932): share groups, implicit/explicit acks, RELEASE/REJECT, acquisition locks",
                new io.kafkatweaks.consumer.ConsumerShareDemo());
        register("client-resilience", "12", "client.rack follower fetching, a broker dying mid-stream, interceptors, KIP-714 telemetry",
                new io.kafkatweaks.consumer.ClientResilienceDemo());
        register("avro-roundtrip", "13", "Schema Registry + Avro: generated records, wire size vs JSON, auto.register/use.latest/subject strategies, evolution",
                new io.kafkatweaks.avro.AvroDemo());
    }

    private Run() {
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0 || argv[0].equals("help") || argv[0].equals("list")) {
            logList();
            return;
        }
        Entry entry = DEMOS.get(argv[0]);
        if (entry == null) {
            log.error("unknown demo '{}'", argv[0]);
            logList();
            System.exit(2);
        }
        Args args = Args.parse(argv, 1);
        log.info("== {}  (chapter {})\n   {}\n   bootstrap.servers={}   args={}",
                argv[0], entry.chapter(), entry.summary(), Env.bootstrapServers(), args);
        entry.demo().run(args);
        // Kafka clients leave daemon threads behind; make sure the JVM (and the exec plugin) exits promptly.
        System.exit(0);
    }

    private static void logList() {
        var demos = new Table("demo", "ch.", "what it shows");
        DEMOS.forEach((name, e) -> demos.row(name, e.chapter(), e.summary()));
        log.info("usage: ./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args=\"<demo> [key=value ...]\"\n{}", demos);
    }

    static void register(String name, String chapter, String summary, Demo demo) {
        DEMOS.put(name, new Entry(chapter, summary, demo));
    }
}
