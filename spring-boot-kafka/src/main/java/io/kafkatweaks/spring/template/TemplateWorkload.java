package io.kafkatweaks.spring.template;

import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Workload;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The plain chapters' {@link Workload} driven through a {@link KafkaTemplate}: same records (null keys, JSON of
 * about {@code sizeBytes}), same result record, so {@code Workload.printComparison} can put a Spring run next to
 * a plain one. {@code template.metrics()} is the underlying producer's {@code metrics()}, so the metric columns
 * are the same too.
 */
public final class TemplateWorkload {

    private TemplateWorkload() {
    }

    public static Workload.Result run(String label, KafkaTemplate<String, String> template, String topic, long records, int sizeBytes) {
        long[] latenciesNanos = new long[(int) records];
        var errors = new AtomicInteger();
        var perPartition = new ConcurrentHashMap<Integer, Integer>();
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>((int) records);
        long bytes = 0;
        var stopwatch = Stopwatch.start();
        for (int i = 0; i < records; i++) {
            String value = Payloads.json(i, sizeBytes);
            bytes += Payloads.utf8Length(value);
            final int index = i;
            final long sentAt = System.nanoTime();
            CompletableFuture<SendResult<String, String>> future = template.send(topic, null, value);
            future.whenComplete((result, ex) -> {
                latenciesNanos[index] = System.nanoTime() - sentAt;
                if (ex != null) {
                    errors.incrementAndGet();
                } else {
                    perPartition.merge(result.getRecordMetadata().partition(), 1, Integer::sum);
                }
            });
            futures.add(future);
        }
        template.flush();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        double elapsedMs = stopwatch.elapsedMillis();
        Arrays.sort(latenciesNanos);
        Map<String, Double> metrics = MetricsReport.snapshot(template.metrics(), MetricsReport.PRODUCER, Workload.METRICS.toArray(String[]::new));
        // Workload's own percentile/max helpers, so a Spring run and a plain run are computed the same way
        // (and an empty run reports NaN instead of throwing).
        return new Workload.Result(label, records, bytes, elapsedMs, errors.get(),
                Workload.percentileMs(latenciesNanos, 0.50), Workload.percentileMs(latenciesNanos, 0.99),
                Workload.maxMs(latenciesNanos), perPartition, metrics);
    }
}
