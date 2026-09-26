package io.kafkatweaks.spring;

import io.kafkatweaks.common.MetricsReport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a handle on every KafkaConsumer / KafkaProducer the auto-configured factories create, so the demos can
 * log the raw client metrics ({@code consumer.metrics()}) through the same {@link MetricsReport} the plain
 * chapters use. Boot applies these customizers to its {@code DefaultKafkaConsumerFactory} /
 * {@code DefaultKafkaProducerFactory} beans; the factory {@code Listener} is the extension point spring-kafka
 * offers for this (its Micrometer binding works the same way).
 * <p>
 * Ids are {@code <factory bean name>.<client.id>}, e.g. {@code kafkaConsumerFactory.spring-tweaks-0}.
 */
@Component
public class ClientCapture implements DefaultKafkaConsumerFactoryCustomizer, DefaultKafkaProducerFactoryCustomizer {

    private final Map<String, Consumer<?, ?>> consumers = new ConcurrentHashMap<>();
    private final Map<String, Producer<?, ?>> producers = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void customize(DefaultKafkaConsumerFactory<?, ?> consumerFactory) {
        ((DefaultKafkaConsumerFactory) consumerFactory).addListener(new ConsumerFactory.Listener<Object, Object>() {
            @Override
            public void consumerAdded(String id, Consumer<Object, Object> consumer) {
                consumers.put(id, consumer);
            }

            @Override
            public void consumerRemoved(String id, Consumer<Object, Object> consumer) {
                consumers.remove(id);
            }
        });
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void customize(DefaultKafkaProducerFactory<?, ?> producerFactory) {
        ((DefaultKafkaProducerFactory) producerFactory).addListener(new ProducerFactory.Listener<Object, Object>() {
            @Override
            public void producerAdded(String id, Producer<Object, Object> producer) {
                producers.put(id, producer);
            }

            @Override
            public void producerRemoved(String id, Producer<Object, Object> producer) {
                producers.remove(id);
            }
        });
    }

    public Map<String, Consumer<?, ?>> consumers() {
        return Map.copyOf(consumers);
    }

    public Map<String, Producer<?, ?>> producers() {
        return Map.copyOf(producers);
    }

    /** Live consumers whose id contains the text (a listener's {@code clientIdPrefix}, for instance). */
    public List<Consumer<?, ?>> consumers(String idContains) {
        return consumers.entrySet().stream().filter(e -> e.getKey().contains(idContains)).map(Map.Entry::getValue).toList();
    }

    /** Sum of one metric over all live consumers matching the id text (e.g. {@code commit-total} over a concurrent container). */
    public double sumConsumerMetric(String idContains, String group, String name) {
        return consumers(idContains).stream().mapToDouble(c -> MetricsReport.value(c.metrics(), group, name)).sum();
    }
}
