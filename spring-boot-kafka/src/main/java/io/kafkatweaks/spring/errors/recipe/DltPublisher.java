package io.kafkatweaks.spring.errors.recipe;

import io.kafkatweaks.common.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Chapter 18 · The template {@code DeadLetterPublishingRecoverer} publishes with. It cannot be the auto-configured one: a
 * record that failed in the deserializer reaches the recoverer as the ORIGINAL {@code byte[]}, a record that
 * failed in the listener as the deserialized {@link Order}, and a {@code JacksonJsonSerializer} would choke on
 * the bytes. {@link DelegatingByTypeSerializer} picks a serializer per value class.
 * <p>
 * Neither the factory nor the template are beans: a second {@code ProducerFactory} or {@code KafkaTemplate}
 * bean would switch Boot's auto-configured ones off (chapter 15). Wrapping them in a bean of another type keeps
 * the lifecycle (closed with the context) without that side effect.
 */
@Component
@Profile("spring-error-handling")
public class DltPublisher implements DisposableBean {

    private final DefaultKafkaProducerFactory<Object, Object> factory;
    private final KafkaTemplate<Object, Object> template;

    public DltPublisher(ProducerFactory<Object, Object> bootFactory) {
        Map<String, Object> configs = new HashMap<>(bootFactory.getConfigurationProperties());
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-errors-dlt");
        var byType = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                String.class, new StringSerializer(),
                Order.class, new JacksonJsonSerializer<Order>()));
        this.factory = new DefaultKafkaProducerFactory<>(configs, null, byType);   // key serializer from the configs (String)
        this.template = new KafkaTemplate<>(factory);
    }

    public KafkaTemplate<Object, Object> template() {
        return template;
    }

    @Override
    public void destroy() {
        factory.destroy();
    }
}
