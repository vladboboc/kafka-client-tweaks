package io.kafkatweaks.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The one workload every producer chapter reuses: send {@code records} messages of {@code sizeBytes}
 * as fast as the producer allows, wait for every acknowledgement, and report what happened.
 * <p>
 * Keeping the workload identical across chapters is the point. When only the configuration changes,
 * the difference in the numbers is the effect of the configuration.
 */
public final class Workload {

    /** The metrics every producer run reports, in the order they appear in the comparison tables. */
    public static final List<String> METRICS = List.of(
            "record-send-rate", "batch-size-avg", "records-per-request-avg", "compression-rate-avg",
            "request-latency-avg", "record-queue-time-avg", "request-rate", "record-retry-total", "record-error-total");

    public enum Payload { JSON, RANDOM }

    public record Result(String label, long records, long bytes, double elapsedMs, int errors,
                         double p50Ms, double p99Ms, double maxMs,
                         Map<Integer, Integer> perPartition, Map<String, Double> metrics) {

        public double recordsPerSecond() {
            return records / (elapsedMs / 1000d);
        }

        public double megabytesPerSecond() {
            return bytes / 1_048_576d / (elapsedMs / 1000d);
        }
    }

    private Workload() {
    }

    /**
     * JIT warm-up with client defaults: payload generation, serializer and codec paths get compiled before
     * the first measured preset, so the first row of a comparison is not penalised for going first.
     */
    public static void warmUp(String topic) {
        run("warm-up", Env.producer("warm-up"), topic, 5_000, 512, Payload.JSON, 0);
    }

    /**
     * Runs the workload on a fresh producer built from {@code props} and closes it afterwards.
     *
     * @param label         name of this run in the tables
     * @param keyCardinality number of distinct keys (0 = null keys, which lets the sticky partitioner do its thing)
     */
    public static Result run(String label, Properties props, String topic, long records, int sizeBytes,
                             Payload payload, int keyCardinality) {
        return run(label, props, topic, records, sizeBytes, payload, keyCardinality, 0);
    }

    /**
     * Same, but paced at {@code ratePerSecond} records/s (0 = as fast as possible). A paced run is how the
     * batching behaviour of a producer under moderate, steady traffic is observed: batches close because
     * linger.ms expires, not because they are full.
     */
    public static Result run(String label, Properties props, String topic, long records, int sizeBytes,
                             Payload payload, int keyCardinality, int ratePerSecond) {
        long intervalNanos = ratePerSecond > 0 ? 1_000_000_000L / ratePerSecond : 0;
        var latenciesNanos = new long[(int) records];
        var errors = new AtomicInteger();
        var bytes = new AtomicLong();
        var perPartition = new ConcurrentHashMap<Integer, Integer>();

        try (var producer = new KafkaProducer<String, String>(props)) {
            // A first send pays for metadata discovery and connection setup; keep it out of the timing.
            producer.partitionsFor(topic);

            var watch = Stopwatch.start();
            long next = System.nanoTime();
            for (int i = 0; i < records; i++) {
                if (intervalNanos > 0) {
                    while (System.nanoTime() < next) {
                        java.util.concurrent.locks.LockSupport.parkNanos(50_000);
                    }
                    next += intervalNanos;
                }
                String key = keyCardinality > 0 ? Payloads.key(i, keyCardinality) : null;
                String value = payload == Payload.JSON ? Payloads.json(i, sizeBytes) : Payloads.random(sizeBytes);
                final int idx = i;
                final long sentAt = System.nanoTime();
                // send() is asynchronous: it returns as soon as the record sits in the accumulator. The
                // callback fires from the sender thread when the broker acknowledged (or gave up on) the batch.
                producer.send(new ProducerRecord<>(topic, key, value), (RecordMetadata md, Exception ex) -> {
                    latenciesNanos[idx] = System.nanoTime() - sentAt;
                    if (ex != null) {
                        errors.incrementAndGet();
                        return;
                    }
                    bytes.addAndGet(md.serializedValueSize() + Math.max(0, md.serializedKeySize()));
                    perPartition.merge(md.partition(), 1, Integer::sum);
                });
            }
            // Blocks until every batch in the accumulator has been sent and acknowledged.
            producer.flush();
            double elapsedMs = watch.elapsedMillis();

            Arrays.sort(latenciesNanos);
            return new Result(label, records, bytes.get(), elapsedMs, errors.get(),
                    percentileMs(latenciesNanos, 0.50), percentileMs(latenciesNanos, 0.99),
                    maxMs(latenciesNanos),
                    new java.util.TreeMap<>(perPartition),
                    MetricsReport.snapshot(producer.metrics(), MetricsReport.PRODUCER, METRICS.toArray(String[]::new)));
        }
    }

    /** Throughput and latency of a single run. */
    public static void printSummary(Result r) {
        new Table("run", "records", "payload MB", "elapsed ms", "records/s", "MB/s", "ack p50 ms", "ack p99 ms", "ack max ms", "errors")
                .row(r.label(), r.records(), r.bytes() / 1_048_576d, r.elapsedMs(), r.recordsPerSecond(),
                        r.megabytesPerSecond(), r.p50Ms(), r.p99Ms(), r.maxMs(), r.errors())
                .print("throughput & end-to-end ack latency");
    }

    /** Several runs side by side: throughput/latency first, then the producer metrics. */
    public static void printComparison(List<Result> results) {
        var summary = new Table("run", "records/s", "MB/s", "ack p50 ms", "ack p99 ms", "errors");
        results.forEach(r -> summary.row(r.label(), r.recordsPerSecond(), r.megabytesPerSecond(), r.p50Ms(), r.p99Ms(), r.errors()));
        summary.print("throughput & latency per configuration");

        var runs = new LinkedHashMap<String, Map<String, Double>>();
        results.forEach(r -> runs.put(r.label(), r.metrics()));
        MetricsReport.printComparison("producer metrics per configuration (producer-metrics group)", METRICS, runs);
    }

    public static void printPartitionSpread(Result r) {
        var table = new Table("partition", "records", "share");
        r.perPartition().forEach((p, n) -> table.row(p, n, "%.1f%%".formatted(100d * n / r.records())));
        table.print("records per partition");
    }

    /**
     * The {@code p}-th percentile of a SORTED array of nanosecond latencies, in milliseconds. Public because
     * every place that measures latencies needs exactly this, including the empty-run guard: a run with no
     * records (e.g. {@code records=0}) has no latencies, and NaN is the answer, not an exception.
     */
    public static double percentileMs(long[] sorted, double p) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        int idx = (int) Math.min(sorted.length - 1, Math.round(p * (sorted.length - 1)));
        return sorted[idx] / 1_000_000d;
    }

    /** The largest value of a SORTED array of nanosecond latencies, in milliseconds; NaN for an empty run. */
    public static double maxMs(long[] sorted) {
        return sorted.length == 0 ? Double.NaN : sorted[sorted.length - 1] / 1_000_000d;
    }
}
