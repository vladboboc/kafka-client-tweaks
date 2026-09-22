package io.kafkatweaks.spring;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The full Boot wiring against a real broker: spring-kafka-test starts a one-node KRaft cluster in the JVM and points
 * {@code spring.kafka.bootstrap-servers} at it. The auto-configured {@code KafkaTemplate} sends, a test-scoped
 * {@code @KafkaListener} receives, and a plain consumer reads the same records back the spring-kafka-test way.
 * Listener auto-startup is off for every profile (application.yml), so the test starts its container itself.
 */
@TweaksSpringTest
@EmbeddedKafka(partitions = 3, topics = TemplateListenerRoundTripTest.TOPIC, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class TemplateListenerRoundTripTest {

    static final String TOPIC = "test.roundtrip";

    @TestConfiguration(proxyBeanMethods = false)
    static class Listeners {
        final CountDownLatch latch = new CountDownLatch(3);
        final List<String> received = new CopyOnWriteArrayList<>();

        @KafkaListener(id = "test-roundtrip", groupId = "test-roundtrip", topics = TOPIC)
        void on(ConsumerRecord<String, String> record) {
            received.add(record.key() + "=" + record.value());
            latch.countDown();
        }
    }

    @Autowired
    KafkaTemplate<String, String> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    Listeners listeners;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Test
    void recordsSentWithTheTemplateReachTheListener() throws Exception {
        MessageListenerContainer container = registry.getListenerContainer("test-roundtrip");
        container.start();
        ContainerTestUtils.waitForAssignment(container, 3);   // one consumer, all three partitions

        for (int i = 1; i <= 3; i++) {
            template.send(TOPIC, "k" + i, "v" + i).get(10, TimeUnit.SECONDS);
        }

        assertThat(listeners.latch.await(30, TimeUnit.SECONDS)).as("listener received all records").isTrue();
        assertThat(listeners.received).containsExactlyInAnyOrder("k1=v1", "k2=v2", "k3=v3");

        // The same records, read back with the helpers spring-kafka-test offers for assertions without a listener.
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-verify", false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);   // consumerProps() assumes Integer keys
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            broker.consumeFromAnEmbeddedTopic(consumer, TOPIC);
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 3);
            assertThat(records.count()).isEqualTo(3);
        }
        container.stop();
    }
}
