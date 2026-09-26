package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import io.kafkatweaks.producer.recipe.LowLatencyProducer;
import io.kafkatweaks.producer.recipe.ThroughputProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

/**
 * Chapter 05: latency first. Measures {@link LowLatencyProducer}; everything else in this file is measurement.
 * Two workloads that look nothing like chapter 02's firehose:
 * <ol>
 *   <li>request/response style: one record at a time, wait for the ack, measure it</li>
 *   <li>a paced stream (N records/s) with async sends, measuring the ack latency of each record</li>
 * </ol>
 * under several presets, from "everything for throughput" to "everything for latency".
 * <pre>
 *   sync-records=300     records for the sequential run
 *   rate=500             records/s for the paced run
 *   seconds=10           duration of the paced run
 *   size=256
 * </pre>
 */
public final class ProducerLowLatencyDemo implements Demo {

    private static final Logger log = LoggerFactory.getLogger(ProducerLowLatencyDemo.class);
    private static final String TOPIC = "tweaks.latency";

    static final Map<String, Map<String, ?>> PRESETS = new LinkedHashMap<>();

    static {
        // From "everything for throughput" (chapter 02) to the recipe under test.
        PRESETS.put("throughput-tuned (linger=50, zstd)", ThroughputProducer.batching(50, 128 * 1024, "zstd"));
        PRESETS.put("defaults (linger=5)", Map.of());
        PRESETS.put("linger=0", LowLatencyProducer.lowLatency());
        PRESETS.put("linger=0, acks=1", LowLatencyProducer.lowLatencyLeaderAck());
    }

    @Override
    public void run(Args args) {
        int syncRecords = args.getInt("sync-records", 300);
        int rate = args.getInt("rate", 500);
        int seconds = args.getInt("seconds", 10);
        int size = args.getInt("size", 256);

        try (var topics = new Topics()) {
            topics.ensure(TOPIC, 3);
        }

        log.info("1. sequential send().get(): each record pays the full round trip, plus linger.ms if the batch waits.");
        var sync = new Table("preset", "p50 ms", "p99 ms", "max ms");
        PRESETS.forEach((label, overrides) -> {
            long[] lat = sequential(props(args, "sync", overrides), syncRecords, size);
            sync.row(label, pct(lat, .5), pct(lat, .99), Workload.maxMs(lat));
        });
        log.info("ack latency, sequential sends ({} records)\n{}", syncRecords, sync);

        log.info("2. paced async stream at {} records/s for {}s: latency each record sees vs. how well batches fill.", rate, seconds);
        var paced = new Table("preset", "sent", "p50 ms", "p99 ms", "batch-size-avg", "records/request", "request-latency-avg", "queue-time-avg");
        PRESETS.forEach((label, overrides) -> paced.row(pacedRow(props(args, "paced", overrides), rate, seconds, size, label)));
        log.info("paced stream\n{}", paced);

        log.info("""
                reading it
                  linger.ms is a floor on latency only while batches are not filling up on their own: at low rates a
                  batch never reaches batch.size, so it waits the full linger before it goes.
                  acks=1 removes the replication round trip from the ack, at the durability price of chapter 03.
                  compression adds CPU time per batch; at a few hundred records/s it is noise, at a firehose it is not.
                  the lowest latency setup is linger.ms=0 + acks=all + idempotence: one record per request, still safe.
                  request-latency-avg is the broker round trip and the part no producer setting can remove.""");
    }

    private static long[] sequential(Properties props, int records, int size) {
        var lat = new long[records];
        try (var producer = new KafkaProducer<String, String>(props)) {
            LowLatencyProducer.warmUp(producer, TOPIC);
            for (int i = 0; i < records; i++) {
                long t0 = System.nanoTime();
                try {
                    producer.send(new ProducerRecord<>(TOPIC, Payloads.key(i, 10), Payloads.json(i, size))).get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                lat[i] = System.nanoTime() - t0;
            }
        }
        Arrays.sort(lat);
        return lat;
    }

    private static Object[] pacedRow(Properties props, int rate, int seconds, int size) {
        int total = Math.max(0, rate) * seconds;
        var lat = new long[total];
        long intervalNanos = rate > 0 ? 1_000_000_000L / rate : 0;   // rate=0: unpaced, so there is nothing to send here
        try (var producer = new KafkaProducer<String, String>(props)) {
            LowLatencyProducer.warmUp(producer, TOPIC);
            long next = System.nanoTime();
            for (int i = 0; i < total; i++) {
                while (System.nanoTime() < next) {
                    LockSupport.parkNanos(50_000);
                }
                next += intervalNanos;
                final int idx = i;
                final long sentAt = System.nanoTime();
                producer.send(new ProducerRecord<>(TOPIC, Payloads.key(i, 10), Payloads.json(i, size)),
                        (RecordMetadata md, Exception ex) -> lat[idx] = System.nanoTime() - sentAt);
            }
            producer.flush();
            var m = producer.metrics();
            Arrays.sort(lat);
            return new Object[] {
                    total, pct(lat, .5), pct(lat, .99),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "batch-size-avg"),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "records-per-request-avg"),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "request-latency-avg"),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "record-queue-time-avg"),
            };
        }
    }

    private static Object[] pacedRow(Properties props, int rate, int seconds, int size, String label) {
        var row = pacedRow(props, rate, seconds, size);
        var out = new Object[row.length + 1];
        out[0] = label;
        System.arraycopy(row, 0, out, 1, row.length);
        return out;
    }

    private static double pct(long[] sorted, double p) {
        return Workload.percentileMs(sorted, p);   // shared, and NaN instead of an exception for an empty run
    }

    private static Properties props(Args args, String clientId, Map<String, ?> overrides) {
        var p = Env.producer("latency-" + clientId);
        p.putAll(overrides);
        return args.applyOverrides(p);
    }
}
