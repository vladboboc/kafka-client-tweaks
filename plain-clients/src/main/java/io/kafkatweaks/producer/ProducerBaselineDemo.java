package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Chapter 01: a producer with nothing tuned. Shows the send path, what one synchronous send costs,
 * what the default configuration is, and how to read the producer's own metrics. Every later chapter
 * changes one group of knobs and compares against this.
 *
 * <pre>
 *   records=20000   how many records to send
 *   size=512        payload size in bytes (order-like JSON)
 *   topic=tweaks.baseline
 * </pre>
 */
public final class ProducerBaselineDemo implements Demo {

    public static final String[] KNOBS = {
            ProducerConfig.ACKS_CONFIG, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ProducerConfig.RETRIES_CONFIG,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            ProducerConfig.LINGER_MS_CONFIG, ProducerConfig.BATCH_SIZE_CONFIG, ProducerConfig.COMPRESSION_TYPE_CONFIG,
            ProducerConfig.BUFFER_MEMORY_CONFIG, ProducerConfig.MAX_BLOCK_MS_CONFIG, ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
            ProducerConfig.PARTITIONER_CLASS_CONFIG, ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG,
            CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
    };

    @Override
    public void run(Args args) {
        String topic = args.get("topic", "tweaks.baseline");
        long records = args.getLong("records", 20_000);
        int size = args.getInt("size", 512);

        try (var topics = new Topics()) {
            topics.ensure(topic, 3);
        }

        Properties props = args.applyOverrides(Env.producer("baseline-producer"));
        Knobs.printProducer(props, KNOBS);

        // --- 1. One synchronous send: the slowest possible way to use a producer, and the clearest. ---
        try (var producer = new KafkaProducer<String, String>(props)) {
            long t0 = System.nanoTime();
            RecordMetadata md = producer.send(new ProducerRecord<>(topic, "warm-up", "hello")).get();
            double firstMs = (System.nanoTime() - t0) / 1_000_000d;
            t0 = System.nanoTime();
            md = producer.send(new ProducerRecord<>(topic, "warm-up", "hello again")).get();
            double secondMs = (System.nanoTime() - t0) / 1_000_000d;
            System.out.printf("%nsynchronous send #1: %.1f ms (includes metadata fetch + connection setup)%n", firstMs);
            System.out.printf("synchronous send #2: %.1f ms  -> %s-%d@%d%n", secondMs, md.topic(), md.partition(), md.offset());
            System.out.println("send().get() per record caps you at roughly 1000 / round-trip-ms records per second. Never do this in a loop.");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        // --- 2. The asynchronous workload every other chapter reuses. ---
        var result = Workload.run("baseline", props, topic, records, size, Workload.Payload.JSON, 0);
        Workload.printSummary(result);
        Workload.printPartitionSpread(result);

        // --- 3. The producer's own view of what happened. ---
        try (var producer = new KafkaProducer<String, String>(props)) {
            // Fresh producer, so these are zero: shown once to make the point that metrics are per instance.
            MetricsReport.print("metrics of a producer that has not sent anything (per-instance, mostly NaN/0)",
                    producer.metrics(), MetricsReport.PRODUCER, "record-send-total", "batch-size-avg", "buffer-available-bytes");
        }
        System.out.println("\nmetrics of the producer that ran the workload:");
        new io.kafkatweaks.common.Table("metric (producer-metrics)", "value", "meaning")
                .row("record-send-rate", result.metrics().get("record-send-rate"), "records/s the sender thread pushed out")
                .row("batch-size-avg", result.metrics().get("batch-size-avg"), "bytes per batch actually sent (compressed)")
                .row("records-per-request-avg", result.metrics().get("records-per-request-avg"), "records per produce request")
                .row("compression-rate-avg", result.metrics().get("compression-rate-avg"), "compressed/uncompressed; 1.0 = none")
                .row("request-latency-avg", result.metrics().get("request-latency-avg"), "ms from request out to broker response")
                .row("record-queue-time-avg", result.metrics().get("record-queue-time-avg"), "ms a record waited in the accumulator")
                .row("request-rate", result.metrics().get("request-rate"), "produce requests/s")
                .row("record-retry-total", result.metrics().get("record-retry-total"), "records re-sent after a retriable error")
                .row("record-error-total", result.metrics().get("record-error-total"), "records that failed for good")
                .print();
    }
}
