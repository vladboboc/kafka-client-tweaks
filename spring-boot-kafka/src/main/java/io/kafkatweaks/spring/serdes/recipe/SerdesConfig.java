package io.kafkatweaks.spring.serdes.recipe;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.converter.JacksonJsonMessageConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Chapter 21 · The application's default serialization is JSON, entirely from application-spring-serdes.yml. The other
 * ways to get a typed value in or out need their own factory: the same {@code spring.kafka.*} properties with the
 * (de)serializer swapped, and for consumers Boot's configurer, so that {@code spring.kafka.listener.*} (auto-startup,
 * poll timeout ...) still applies. Listeners pick a factory with {@code containerFactory = "..."}.
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-serdes")
public class SerdesConfig {

    /** The consumer properties without the JSON deserializer's own settings, which the other deserializers would only log as unknown. */
    static Map<String, Object> consumerConfigsWithoutJson(KafkaProperties properties) {
        Map<String, Object> configs = new HashMap<>(properties.buildConsumerProperties());
        configs.keySet().removeIf(k -> k.startsWith("spring.json."));
        return configs;
    }

    /**
     * Values stay Strings until the listener is called; a {@code JacksonJsonMessageConverter} then converts the payload
     * to the listener METHOD's parameter type. One topic with several event types can be served by a class-level
     * {@code @KafkaListener} with one {@code @KafkaHandler} per type this way.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> converterContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, KafkaProperties properties) {
        Map<String, Object> configs = consumerConfigsWithoutJson(properties);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, new DefaultKafkaConsumerFactory<>(configs));
        factory.setRecordMessageConverter(new JacksonJsonMessageConverter());
        return factory;
    }

    /**
     * Confluent Avro in: {@code KafkaAvroDeserializer} reads the schema id, fetches the writer schema from the registry
     * ({@code schema.registry.url} comes from {@code spring.kafka.properties}) and, with {@code specific.avro.reader},
     * instantiates the generated class. Generated classes must be trusted first (chapter 13, {@code AvroTrust}).
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> avroContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, KafkaProperties properties) {
        Map<String, Object> configs = consumerConfigsWithoutJson(properties);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, new DefaultKafkaConsumerFactory<>(configs));
        return factory;
    }

    /**
     * Confluent Avro out: a producer factory from the same {@code spring.kafka.producer.*} with the value serializer
     * swapped ({@code schema.registry.url} is already in the map). Not a bean: a second {@code ProducerFactory} bean would
     * switch Boot's auto-configured one off. Wrap it in {@code new KafkaTemplate<>(factory)} and {@code destroy()} the
     * factory when done. An Avro-only application would instead set the serializers on the default factories
     * (commented at the end of application-spring-serdes.yml).
     */
    public static DefaultKafkaProducerFactory<String, Object> avroProducerFactory(KafkaProperties properties, String clientId) {
        Map<String, Object> configs = new HashMap<>(properties.buildProducerProperties());
        configs.keySet().removeIf(k -> k.startsWith("spring.json."));
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }
}
