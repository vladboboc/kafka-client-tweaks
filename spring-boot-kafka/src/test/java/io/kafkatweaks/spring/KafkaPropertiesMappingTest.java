package io.kafkatweaks.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chapter 14's mapping table, checked against Boot itself and without a Spring context: the same {@code Binder}
 * that reads application.yml binds a map of properties into {@link KafkaProperties}, and
 * {@code buildProducerProperties()} / {@code buildConsumerProperties()} are what the auto-configured factories get.
 */
class KafkaPropertiesMappingTest {

    private static KafkaProperties bind(Map<String, String> properties) {
        return new Binder(new MapConfigurationPropertySource(properties)).bind("spring.kafka", KafkaProperties.class).get();
    }

    @Test
    void typedProducerKeysBecomeClientConfigs() {
        Map<String, Object> configs = bind(Map.of(
                "spring.kafka.bootstrap-servers", "localhost:19092,localhost:29092",
                "spring.kafka.client-id", "mapping-test",
                "spring.kafka.producer.acks", "all",
                "spring.kafka.producer.batch-size", "64KB",
                "spring.kafka.producer.compression-type", "zstd",
                "spring.kafka.producer.retries", "10",
                "spring.kafka.producer.properties.linger.ms", "50",
                "spring.kafka.producer.properties.enable.idempotence", "true")).buildProducerProperties();

        assertThat(configs)
                .containsEntry("bootstrap.servers", List.of("localhost:19092", "localhost:29092"))
                .containsEntry("client.id", "mapping-test")
                .containsEntry("acks", "all")
                .containsEntry("compression.type", "zstd")
                .containsEntry("linger.ms", "50")
                .containsEntry("enable.idempotence", "true");
        assertThat(String.valueOf(configs.get("batch.size"))).isEqualTo("65536");   // DataSize -> bytes
        assertThat(String.valueOf(configs.get("retries"))).isEqualTo("10");
    }

    @Test
    void typedConsumerKeysBecomeClientConfigs() {
        Map<String, Object> configs = bind(Map.of(
                "spring.kafka.consumer.group-id", "mapping-test",
                "spring.kafka.consumer.auto-offset-reset", "earliest",
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.consumer.fetch-min-size", "1MB",
                "spring.kafka.consumer.fetch-max-wait", "250ms",
                "spring.kafka.consumer.max-poll-records", "200",
                "spring.kafka.consumer.isolation-level", "read_committed",
                "spring.kafka.consumer.properties.group.protocol", "consumer")).buildConsumerProperties();

        assertThat(configs)
                .containsEntry("group.id", "mapping-test")
                .containsEntry("auto.offset.reset", "earliest")
                .containsEntry("isolation.level", "read_committed")
                .containsEntry("group.protocol", "consumer");
        assertThat(String.valueOf(configs.get("enable.auto.commit"))).isEqualTo("false");
        assertThat(String.valueOf(configs.get("fetch.min.bytes"))).isEqualTo("1048576");   // DataSize -> bytes
        assertThat(String.valueOf(configs.get("fetch.max.wait.ms"))).isEqualTo("250");     // Duration -> ms
        assertThat(String.valueOf(configs.get("max.poll.records"))).isEqualTo("200");
    }

    @Test
    void commonPropertiesReachBothClientsAndPerClientPropertiesWin() {
        KafkaProperties properties = bind(Map.of(
                "spring.kafka.properties.metadata.max.age.ms", "1000",
                "spring.kafka.properties.schema.registry.url", "http://localhost:8081",
                "spring.kafka.producer.properties.metadata.max.age.ms", "2000"));

        assertThat(properties.buildProducerProperties())
                .containsEntry("metadata.max.age.ms", "2000")            // producer.properties over spring.kafka.properties
                .containsEntry("schema.registry.url", "http://localhost:8081");
        assertThat(properties.buildConsumerProperties())
                .containsEntry("metadata.max.age.ms", "1000")
                .containsEntry("schema.registry.url", "http://localhost:8081");   // why chapter 21 puts it there once
    }

    @Test
    void freeFormPropertiesOverrideTheTypedKeyForTheSameConfig() {
        // Both name the same client config; Boot applies the typed keys first and the properties map last.
        Map<String, Object> configs = bind(Map.of(
                "spring.kafka.producer.acks", "all",
                "spring.kafka.producer.properties.acks", "1")).buildProducerProperties();

        assertThat(configs).containsEntry("acks", "1");
    }
}
