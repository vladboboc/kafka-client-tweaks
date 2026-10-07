package io.kafkatweaks;

import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Single entry point for every demo, so each chapter can be run with the same command shape:
 * <pre>
 *   ./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-batching records=20000 compression=zstd"
 * </pre>
 * The chapter number works in place of the name ({@code 02}), which is what the {@code demo} script at the repository
 * root passes. Run without arguments to list the demos. Kafka client properties can be passed as extra
 * {@code key=value} pairs; every key containing a dot is applied on top of the demo's own configuration.
 */
public final class Run {

    private static final Logger log = LoggerFactory.getLogger(Run.class);

    /** @param doc the chapter's file in {@code docs/} */
    record Entry(String name, String chapter, String doc, String summary, Demo demo) {
    }

    private static final Map<String, Entry> DEMOS = new LinkedHashMap<>();

    static {
        // One line per chapter, in the order of the docs.
        register("producer-baseline", "01", "01-producer-baseline.md",
                "untuned producer: send path, sync vs async send, default knobs, reading metrics",
                new io.kafkatweaks.producer.ProducerBaselineDemo());
        register("producer-batching", "02", "02-producer-batching-compression.md",
                "throughput: batch.size, linger.ms, compression.type/levels, buffer.memory, max.block.ms",
                new io.kafkatweaks.producer.ProducerBatchingDemo());
        register("producer-durability", "03", "03-producer-durability.md",
                "acks 0/1/all, idempotence, the timeout chain, min.insync.replicas with a broker stopped",
                new io.kafkatweaks.producer.ProducerDurabilityDemo());
        register("producer-partitioning", "04", "04-producer-partitioning.md",
                "sticky vs round-robin, keyed hashing, hot keys, partitioner.ignore.keys, custom Partitioner",
                new io.kafkatweaks.producer.ProducerPartitioningDemo());
        register("producer-low-latency", "05", "05-producer-low-latency.md",
                "latency first: linger.ms=0, acks, compression off; sequential and paced workloads",
                new io.kafkatweaks.producer.ProducerLowLatencyDemo());
        register("producer-transactions", "06", "06-producer-transactions.md",
                "atomic writes, read_committed, commit cost, exactly-once consume-transform-produce, zombie fencing",
                new io.kafkatweaks.producer.ProducerTransactionsDemo());
        register("consumer-fetch", "07", "07-consumer-fetch.md",
                "poll loop: max.poll.records, fetch.min/max.bytes, fetch.max.wait.ms, max.poll.interval.ms and a slow handler",
                new io.kafkatweaks.consumer.ConsumerFetchDemo());
        register("consumer-offsets", "08", "08-consumer-offsets.md",
                "commit strategies: at-most-once, at-least-once, idempotent handler, auto-commit timing, auto.offset.reset, seek",
                new io.kafkatweaks.consumer.ConsumerOffsetsDemo());
        register("consumer-rebalance", "09", "09-consumer-rebalance.md",
                "group.protocol consumer (KIP-848) vs classic, assignors, static membership; a live rebalance timeline",
                new io.kafkatweaks.consumer.ConsumerRebalanceDemo());
        register("consumer-parallel", "10", "10-consumer-parallel.md",
                "scaling: partitions vs consumers, per-partition workers on virtual threads, pause/resume back-pressure",
                new io.kafkatweaks.consumer.ConsumerParallelDemo());
        register("consumer-share", "11", "11-consumer-share-groups.md",
                "Queues for Kafka (KIP-932): share groups, implicit/explicit acks, RELEASE/REJECT, acquisition locks",
                new io.kafkatweaks.consumer.ConsumerShareDemo());
        register("client-resilience", "12", "12-client-resilience.md",
                "client.rack follower fetching, a broker dying mid-stream, interceptors, KIP-714 telemetry",
                new io.kafkatweaks.consumer.ClientResilienceDemo());
        register("avro-roundtrip", "13", "13-avro-schema-registry.md",
                "Schema Registry + Avro: generated records, wire size vs JSON, auto.register/use.latest/subject strategies, evolution",
                new io.kafkatweaks.avro.AvroDemo());
    }

    private Run() {
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0 || argv[0].equals("help") || argv[0].equals("list")) {
            logList();
            return;
        }
        Entry entry = find(argv[0]).orElse(null);
        if (entry == null) {
            log.error("unknown demo '{}'", argv[0]);
            logList();
            System.exit(2);
        }
        Args args = Args.parse(argv, 1);
        log.info("== {}  (chapter {}, docs/{})\n   {}\n   bootstrap.servers={}   args={}",
                entry.name(), entry.chapter(), entry.doc(), entry.summary(), Env.bootstrapServers(), args);
        entry.demo().run(args);
        // Kafka clients leave daemon threads behind; make sure the JVM (and the exec plugin) exits promptly.
        System.exit(0);
    }

    /** A demo by name ({@code producer-batching}) or by chapter number ({@code 02} or {@code 2}). */
    static Optional<Entry> find(String nameOrChapter) {
        Entry byName = DEMOS.get(nameOrChapter);
        if (byName != null) {
            return Optional.of(byName);
        }
        String chapter = nameOrChapter.length() == 1 ? "0" + nameOrChapter : nameOrChapter;
        return DEMOS.values().stream().filter(e -> e.chapter().equals(chapter)).findFirst();
    }

    static Collection<Entry> all() {
        return DEMOS.values();
    }

    private static void logList() {
        var demos = new Table("demo", "ch.", "what it shows", "read");
        DEMOS.values().forEach(e -> demos.row(e.name(), e.chapter(), e.summary(), "docs/" + e.doc()));
        log.info("usage: ./demo <chapter|demo> [key=value ...]   or   ./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args=\"<demo> [key=value ...]\"\n{}", demos);
    }

    static void register(String name, String chapter, String doc, String summary, Demo demo) {
        DEMOS.put(name, new Entry(name, chapter, doc, summary, demo));
    }
}
