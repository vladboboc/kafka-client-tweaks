package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import io.kafkatweaks.producer.recipe.ThroughputProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Chapter 02: throughput. Measures {@link ThroughputProducer}; everything else in this file is measurement.
 * Runs the same workload under a matrix of batching and compression settings and logs them side by side.
 * Then shows what happens when the accumulator fills up.
 *
 * <pre>
 *   records=30000        records per run
 *   size=512             payload bytes
 *   payload=json|random  compressible JSON (default) or incompressible random text
 *   runs=a,b,c           subset of the preset names below (default: all)
 *   buffer-demo=true     also run the buffer.memory / max.block.ms demonstration
 * </pre>
 */
public final class ProducerBatchingDemo implements Demo {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatchingDemo.class);

    /** Preset name → overrides on top of the client defaults. Order is the order of the tables. */
    static final Map<String, Map<String, ?>> PRESETS = new LinkedHashMap<>();

    static {
        // One knob at a time first: linger.ms alone, then batch.size, to show which one moves the numbers.
        PRESETS.put("linger0", Map.of(ProducerConfig.LINGER_MS_CONFIG, 0));
        PRESETS.put("defaults", Map.of());
        PRESETS.put("linger20", Map.of(ProducerConfig.LINGER_MS_CONFIG, 20));
        PRESETS.put("linger20-batch64k", Map.of(ProducerConfig.LINGER_MS_CONFIG, 20, ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024));
        // The recipe under test: the same batching with each codec, then the chapter's best combination.
        PRESETS.put("lz4", ThroughputProducer.batching(20, 64 * 1024, "lz4"));
        PRESETS.put("snappy", ThroughputProducer.batching(20, 64 * 1024, "snappy"));
        PRESETS.put("zstd", ThroughputProducer.batching(20, 64 * 1024, "zstd"));
        PRESETS.put("zstd-level9", with(ThroughputProducer.batching(20, 64 * 1024, "zstd"), ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 9));
        PRESETS.put("gzip", ThroughputProducer.batching(20, 64 * 1024, "gzip"));
        PRESETS.put("linger100-batch256k-zstd", ThroughputProducer.highThroughput());
    }

    @Override
    public void run(Args args) {
        String topic = args.get("topic", "tweaks.batching");
        long records = args.getLong("records", 30_000);
        int size = args.getInt("size", 512);
        var payload = args.getEnum("payload", Workload.Payload.JSON);
        List<String> runs = args.get("runs").map(s -> Arrays.asList(s.split(","))).orElse(new ArrayList<>(PRESETS.keySet()));

        try (var topics = new Topics()) {
            topics.ensure(topic, 3);
        }

        log.info("payload: {}, {} bytes/record, {} records per run, {} bytes total per run", payload, size, records, records * size);

        Workload.warmUp(topic);

        var results = new ArrayList<Workload.Result>();
        for (String name : runs) {
            Map<String, ?> preset = PRESETS.get(name);
            if (preset == null) {
                log.warn("unknown preset '{}', known: {}", name, PRESETS.keySet());
                continue;
            }
            Properties props = Env.producer("batching-" + name);
            props.putAll(preset);
            args.applyOverrides(props);
            log.info("running {} {}", "%-26s".formatted(name), preset.isEmpty() ? "(client defaults)" : preset);
            results.add(Workload.run(name, props, topic, records, size, payload, 0));
        }
        Workload.logComparison(results);

        log.info("""
                how to read it
                  batch-size-avg          bigger batches = fewer requests per record = less broker work per record
                  records-per-request-avg the same thing from the request side
                  compression-rate-avg    compressed/uncompressed bytes; JSON compresses ~5-10x, random text ~1.0
                  record-queue-time-avg   what linger.ms costs each record in the accumulator
                  ack p99                 the latency the caller sees; batching trades this for records/s""");

        if (args.getBool("buffer-demo", true)) {
            bufferDemo(args, topic, size);
        }
    }

    /**
     * buffer.memory is the total accumulator size. When the application produces faster than the sender
     * thread can drain, send() blocks waiting for buffer space for at most max.block.ms, and then REJECTS the
     * record: since Kafka 3.x the BufferExhaustedException comes back through the record's future and callback,
     * it is not thrown by send() (KafkaProducer.doSend turns every ApiException into a failed future). A producer
     * without a callback therefore loses records silently, which is why the loop below counts them.
     * Same bounded workload twice: with a comfortable buffer and with a tiny one plus a short max.block.ms.
     * The metric that shows the pressure building is bufferpool-wait-ratio (fraction of time appenders spent
     * waiting for memory).
     */
    private static void bufferDemo(Args args, String topic, int size) {
        long records = args.getLong("buffer-records", 60_000);
        var table = new Table("buffer.memory", "max.block.ms", "sent", "outcome", "bufferpool-wait-ratio", "buffer-available-bytes (end)");
        table.row(bufferRun(args, topic, size, records, 32 * 1024 * 1024, 60_000));
        table.row(bufferRun(args, topic, size, records, 1024 * 1024, 20));
        log.info("back-pressure: {} random records of {} bytes, linger.ms=100 so batches sit in the accumulator\n{}", records, size, table);
        log.info("""
                bufferpool-wait-ratio > 0 means send() is already blocking on memory: the app is faster than the network.
                  options: bigger buffer.memory (more latency, not more throughput), shorter linger.ms, compression,
                  more partitions/brokers, or accept it: read the BufferExhaustedException from the send() callback or
                  future and drop / spill / slow the caller. send() itself does not throw it, so a producer that passes
                  no callback and never looks at the future drops those records without noticing.
                  buffer-available-bytes is the gauge to alert on before records start being rejected.""");
    }

    private static Object[] bufferRun(Args args, String topic, int size, long records, long bufferMemory, long maxBlockMs) {
        Properties props = Env.producer("batching-buffer-" + bufferMemory);
        props.putAll(ThroughputProducer.bounded(bufferMemory, maxBlockMs));   // <- the recipe under test
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(64 * 1024));
        args.applyOverrides(props);

        long attempted = 0;
        var failures = new ThroughputProducer.FailureCounter();
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.partitionsFor(topic);
            for (; attempted < records; attempted++) {
                // The callback is the only place a full accumulator becomes visible: send() returns a failed
                // future instead of throwing, so a producer that ignores both counts rejected records as sent.
                producer.send(new ProducerRecord<>(topic, null, Payloads.random(size)), failures);
            }
            producer.flush();   // every callback has run by the time flush() returns
            var m = producer.metrics();
            long rejected = failures.failed();
            long acked = attempted - rejected;
            String outcome = rejected == 0
                    ? "all sent"
                    : "%d of %d rejected: %s".formatted(rejected, attempted, describe(failures.firstFailure()));
            // Counts and config values are exact: format them here so the humanising number formatter cannot round them.
            return new Object[] {String.valueOf(bufferMemory), String.valueOf(maxBlockMs), String.valueOf(acked), outcome,
                    MetricsReport.value(m, MetricsReport.PRODUCER, "bufferpool-wait-ratio"),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "buffer-available-bytes")};
        }
    }

    /** Class name only: the full "Failed to allocate 65536 bytes within ..." message would stretch the table. */
    private static String describe(Exception e) {
        return e == null ? "" : e.getClass().getSimpleName();
    }

    /** A preset plus one more setting, for the variations the recipe does not name. */
    private static Map<String, ?> with(Map<String, ?> preset, String key, Object value) {
        var copy = new LinkedHashMap<String, Object>(preset);
        copy.put(key, value);
        return copy;
    }
}
