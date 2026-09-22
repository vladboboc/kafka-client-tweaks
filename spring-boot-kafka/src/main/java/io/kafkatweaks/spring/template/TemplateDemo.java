package io.kafkatweaks.spring.template;

import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Chapter 15: the {@link KafkaTemplate}.
 * <ol>
 *   <li>send() returns a CompletableFuture: blocking per record vs letting the batches form</li>
 *   <li>a ProducerListener bean sees every acknowledgement</li>
 *   <li>several templates from ONE ProducerFactory: the chapter-02 batching matrix, Spring style</li>
 *   <li>Message&lt;?&gt; with headers, sendDefault()</li>
 *   <li>what Micrometer knows: the spring.kafka.template timer and the bound client metrics</li>
 * </ol>
 * <pre>
 *   records=20000     records per preset in the matrix
 *   sync=1000         records sent one at a time with .get()
 *   size=512          bytes of JSON per record
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-template")
public class TemplateDemo {

    /** Replaces Boot's LoggingProducerListener; Boot hands it to the auto-configured KafkaTemplate. */
    @Bean
    CountingProducerListener producerListener() {
        return new CountingProducerListener();
    }

    @Bean
    ApplicationRunner springTemplate(DemoSupport support, KafkaTemplate<String, String> template,
                                     ProducerFactory<String, String> producerFactory, CountingProducerListener listener,
                                     MeterRegistry registry) {
        return support.demo("spring-template", args -> {
            long records = args.getLong("records", 20_000);
            int sync = args.getInt("sync", 1000);
            int size = args.getInt("size", 512);
            String topic = TopicsConfig.TEMPLATE;
            try (var topics = new Topics()) {
                topics.ensure(topic, 3);
            }
            Workload.warmUp(topic);

            // ---- 1. the future ------------------------------------------------------------------------------
            var modes = new Table("mode", "records", "elapsed ms", "records/s", "how");
            var sw = Stopwatch.start();
            for (int i = 0; i < sync; i++) {
                template.send(topic, Payloads.key(i, 20), Payloads.json(i, size)).get(10, TimeUnit.SECONDS);
            }
            double syncMs = sw.elapsedMillis();
            modes.row("send().get() per record", sync, "%.0f".formatted(syncMs), "%.0f".formatted(sync / syncMs * 1000), "one round trip per record; linger.ms never gets a chance");

            sw = Stopwatch.start();
            List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();
            for (int i = 0; i < sync; i++) {
                futures.add(template.send(topic, Payloads.key(i, 20), Payloads.json(i, size)));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            double asyncMs = sw.elapsedMillis();
            modes.row("send() x N, then allOf().join()", sync, "%.0f".formatted(asyncMs), "%.0f".formatted(sync / asyncMs * 1000), "batches form; whenComplete() for per-record results");
            modes.print("1. the same %d records through the auto-configured template".formatted(sync));
            System.out.println("   send() is always asynchronous. .get() is the application choosing to wait; the plain chapter-01 numbers apply.");

            // ---- 2. ProducerListener -----------------------------------------------------------------------
            System.out.printf("%n2. ProducerListener bean: onSuccess=%d onError=%d (every send of the template above, no code at the call sites)%n",
                    listener.successes(), listener.failures());

            // ---- 3. several templates from one factory ------------------------------------------------------
            Map<String, Map<String, Object>> presets = new LinkedHashMap<>();
            // A client.id per preset: it forces a factory copy even for "defaults" (an empty override map would reuse the
            // bean's producer and its metrics) and it tags the Micrometer meters of each producer.
            presets.put("defaults (linger 5, batch 16K, none)", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "spring-template-defaults"));
            presets.put("linger 50, batch 128K", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "spring-template-batch", ProducerConfig.LINGER_MS_CONFIG, 50, ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024));
            presets.put("linger 50, batch 128K, lz4", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "spring-template-lz4", ProducerConfig.LINGER_MS_CONFIG, 50, ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024, ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"));
            presets.put("linger 50, batch 128K, zstd", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "spring-template-zstd", ProducerConfig.LINGER_MS_CONFIG, 50, ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024, ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
            presets.put("linger 0, acks 1, none (latency first)", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "spring-template-latency", ProducerConfig.LINGER_MS_CONFIG, 0, ProducerConfig.ACKS_CONFIG, "1", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false));
            List<Workload.Result> results = new ArrayList<>();
            for (var preset : presets.entrySet()) {
                // Not a bean on purpose: a second KafkaTemplate bean would switch Boot's auto-configured one off.
                // The constructor copies the factory with the overrides (copyWithConfigurationOverride), so this
                // template has its own KafkaProducer with its own batching settings.
                var tuned = new KafkaTemplate<>(producerFactory, preset.getValue());
                try {
                    results.add(TemplateWorkload.run(preset.getKey(), tuned, topic, records, size));
                } finally {
                    tuned.destroy();   // closes the producer of the copied factory
                }
            }
            System.out.printf("%n3. the chapter-02 matrix through KafkaTemplates built from the one ProducerFactory (%d records x %d bytes each):%n", records, size);
            Workload.printComparison(results);

            // ---- 4. Message<?> and sendDefault ---------------------------------------------------------------
            SendResult<String, String> withHeaders = template.send(MessageBuilder.withPayload(Payloads.json(1, size))
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.KEY, "customer-1")
                    .setHeader("tenant", "acme")
                    .build()).get(10, TimeUnit.SECONDS);
            SendResult<String, String> toDefault = template.sendDefault("customer-2", Payloads.json(2, size)).get(10, TimeUnit.SECONDS);
            var sends = new Table("call", "topic", "partition", "offset", "headers on the record");
            // Offsets are exact values, not magnitudes: String.valueOf keeps them out of the humanising formatter
            // (which would print offset 69 312 as "69.3K").
            sends.row("send(Message<?>) with KafkaHeaders.TOPIC/KEY + tenant", withHeaders.getRecordMetadata().topic(), withHeaders.getRecordMetadata().partition(),
                    String.valueOf(withHeaders.getRecordMetadata().offset()), headers(withHeaders));
            sends.row("sendDefault(key, value)  [spring.kafka.template.default-topic]", toDefault.getRecordMetadata().topic(), toDefault.getRecordMetadata().partition(),
                    String.valueOf(toDefault.getRecordMetadata().offset()), headers(toDefault));
            sends.print("4. the messaging API: Spring Message<?> headers become Kafka record headers (KafkaHeaders.* are consumed by the template)");

            // ---- 5. Micrometer -------------------------------------------------------------------------------
            var meters = new Table("meter", "tags", "count", "mean ms");
            for (Timer timer : registry.find("spring.kafka.template").timers()) {
                meters.row(timer.getId().getName(), tags(timer), timer.count(), "%.3f".formatted(timer.mean(TimeUnit.MILLISECONDS)));
            }
            for (String name : List.of("kafka.producer.record.send.total", "kafka.producer.batch.size.avg", "kafka.producer.compression.rate.avg", "kafka.producer.request.latency.avg")) {
                registry.find(name).meters().forEach(m -> meters.row(name, tagsOf(m.getId()), "%.2f".formatted(m.measure().iterator().next().getValue()), ""));
            }
            meters.print("5. Micrometer: spring-kafka's own timer per template bean + the kafka-clients metrics Boot bound (tag spring.id = factory.client.id)");
            System.out.println("   the timer is per KafkaTemplate BEAN (name tag); the tuned templates above were not beans and have no timer.");
            System.out.println("   spring.kafka.template.observation-enabled=true replaces the timer with an Observation per send (tracing span + metrics).");
        });
    }

    private static String headers(SendResult<String, String> result) {
        var names = new ArrayList<String>();
        result.getProducerRecord().headers().forEach(h -> names.add(h.key()));
        return names.isEmpty() ? "-" : String.join(", ", names);
    }

    private static String tags(Timer timer) {
        return tagsOf(timer.getId());
    }

    private static String tagsOf(io.micrometer.core.instrument.Meter.Id id) {
        var parts = new ArrayList<String>();
        id.getTags().forEach(t -> parts.add(t.getKey() + "=" + t.getValue()));
        return String.join(" ", parts);
    }
}
