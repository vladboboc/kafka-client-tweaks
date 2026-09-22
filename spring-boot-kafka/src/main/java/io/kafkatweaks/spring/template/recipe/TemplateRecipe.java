package io.kafkatweaks.spring.template.recipe;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * Chapter 15 · The auto-configured {@code KafkaTemplate}, one {@code ProducerListener} for every acknowledgement, and
 * templates with other producer settings derived from the same factory.
 * <p>
 * The producer settings of the auto-configured template come from {@code spring.kafka.producer.*}
 * (application-spring-template.yml). Measured by the {@code spring-template} demo (docs/15-spring-kafkatemplate.md),
 * 20 000 records through {@link #derivedTemplate} templates of the one factory:
 * <pre>
 *   client defaults       19.8K records/s   ack p50  502 ms
 *   THROUGHPUT            72.4K records/s   ack p50  5.7 ms   (zstd: batches went out at 6% of their size)
 *   LATENCY_FIRST         32.0K records/s   ack p50  195 ms   (acks=1 buys little without fixing the batching)
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-template")
public class TemplateRecipe {

    /** Chapter 02's throughput settings, as overrides for a derived template. */
    public static final Map<String, Object> THROUGHPUT = Map.of(
            ProducerConfig.LINGER_MS_CONFIG, 50,
            ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024,
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

    /** Chapter 05's latency-first settings (acks=1: only for data you can rebuild). */
    public static final Map<String, Object> LATENCY_FIRST = Map.of(
            ProducerConfig.LINGER_MS_CONFIG, 0,
            ProducerConfig.ACKS_CONFIG, "1",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    /**
     * A {@code ProducerListener} bean replaces Boot's {@code LoggingProducerListener}: Boot hands it to the
     * auto-configured template, which calls it for every acknowledgement and every failure. The place for send metrics,
     * audit counters or alerting, without touching a single call site.
     */
    @Bean
    CountingProducerListener producerListener() {
        return new CountingProducerListener();
    }

    /**
     * A template with its own producer settings from the ONE {@code ProducerFactory}: the constructor copies the factory
     * with the overrides ({@code copyWithConfigurationOverride}), so the new template has its own {@code KafkaProducer}.
     * Deliberately not a bean: a second {@code KafkaTemplate} bean would switch Boot's auto-configured one off. Call
     * {@code destroy()} when done, it closes the copied factory's producer. Give it a {@code client.id} of its own: that
     * tags its metrics, and an empty override map would silently reuse the bean's producer instead of copying.
     */
    public static <K, V> KafkaTemplate<K, V> derivedTemplate(ProducerFactory<K, V> factory, Map<String, Object> overrides) {
        return new KafkaTemplate<>(factory, overrides);
    }
}
