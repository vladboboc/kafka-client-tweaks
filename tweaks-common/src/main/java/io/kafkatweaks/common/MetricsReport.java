package io.kafkatweaks.common;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the client's own metrics ({@code producer.metrics()} / {@code consumer.metrics()}) and logs
 * the handful that matter for a given chapter. This is the same data JMX exposes, without the plumbing.
 * <p>
 * Kafka registers most metrics twice: once aggregated per client (tags = {client-id}) and again per
 * topic / partition / node with extra tags. The helpers here pick the <em>aggregate</em> one, i.e. the
 * metric whose only tag is {@code client-id}, so a name maps to exactly one number.
 *
 * <h2>Metric groups you will meet</h2>
 * <ul>
 *   <li>{@code producer-metrics}: batch-size-avg, records-per-request-avg, record-send-rate,
 *       compression-rate-avg, request-latency-avg, record-queue-time-avg, buffer-available-bytes,
 *       record-retry-total, record-error-total, produce-throttle-time-avg</li>
 *   <li>{@code consumer-fetch-manager-metrics}: records-consumed-rate, fetch-latency-avg, fetch-size-avg,
 *       records-per-request-avg, records-lag-max, bytes-consumed-rate, fetch-rate</li>
 *   <li>{@code consumer-coordinator-metrics}: commit-latency-avg, commit-rate, rebalance-latency-avg,
 *       rebalance-total, assigned-partitions, last-rebalance-seconds-ago</li>
 *   <li>{@code consumer-metrics}: time-between-poll-avg, poll-idle-ratio-avg, last-poll-seconds-ago</li>
 * </ul>
 */
public final class MetricsReport {

    public static final String PRODUCER = "producer-metrics";
    public static final String PRODUCER_TOPIC = "producer-topic-metrics";
    public static final String CONSUMER_FETCH = "consumer-fetch-manager-metrics";
    public static final String CONSUMER_COORDINATOR = "consumer-coordinator-metrics";
    public static final String CONSUMER = "consumer-metrics";
    public static final String SHARE_CONSUMER_FETCH = "consumer-share-fetch-manager-metrics";
    public static final String SHARE_CONSUMER_COORDINATOR = "consumer-share-coordinator-metrics";
    public static final String SHARE_CONSUMER = "consumer-share-metrics";

    private static final Logger log = LoggerFactory.getLogger(MetricsReport.class);

    private MetricsReport() {
    }

    /** The aggregate (client-level) value of one metric, or NaN when the client never recorded it. */
    public static double value(Map<MetricName, ? extends Metric> metrics, String group, String name) {
        for (var e : metrics.entrySet()) {
            MetricName mn = e.getKey();
            if (mn.group().equals(group) && mn.name().equals(name) && isAggregate(mn)) {
                return asDouble(e.getValue());
            }
        }
        return Double.NaN;
    }

    /** Snapshot of several aggregate metrics, in the order given. */
    public static Map<String, Double> snapshot(Map<MetricName, ? extends Metric> metrics, String group, String... names) {
        var out = new LinkedHashMap<String, Double>();
        for (String n : names) {
            out.put(n, value(metrics, group, n));
        }
        return out;
    }

    /** Logs a two-column table (metric, value) for the given aggregate metrics. */
    public static void logMetrics(String title, Map<MetricName, ? extends Metric> metrics, String group, String... names) {
        var table = new Table("metric (" + group + ")", "value");
        for (String n : names) {
            table.row(n, value(metrics, group, n));
        }
        log.info("{}\n{}", title, table);
    }

    /**
     * Logs a comparison: one row per configuration, one column per metric. The first column is the
     * label of the run. Used by the chapters that run the same workload under several configs.
     */
    public static void logComparison(String title, List<String> metricNames, Map<String, Map<String, Double>> runs) {
        var header = new String[metricNames.size() + 1];
        header[0] = "run";
        for (int i = 0; i < metricNames.size(); i++) {
            header[i + 1] = metricNames.get(i);
        }
        var table = new Table(header);
        runs.forEach((label, values) -> {
            var cells = new Object[header.length];
            cells[0] = label;
            for (int i = 0; i < metricNames.size(); i++) {
                cells[i + 1] = values.get(metricNames.get(i));
            }
            table.row(cells);
        });
        log.info("{}\n{}", title, table);
    }

    private static boolean isAggregate(MetricName mn) {
        var tags = mn.tags();
        return tags.size() <= 1 && (tags.isEmpty() || tags.containsKey("client-id"));
    }

    private static double asDouble(Metric metric) {
        Object v = metric.metricValue();
        return v instanceof Number n ? n.doubleValue() : Double.NaN;
    }
}
