package io.kafkatweaks.spring.errors;

import io.kafkatweaks.spring.TweaksSpringTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chapter 18's blocking path as a test: a {@code DefaultErrorHandler} bean (Boot wires a single {@code CommonErrorHandler}
 * bean into its container factory) retries a failing listener twice and then hands the record to a
 * {@code DeadLetterPublishingRecoverer}, which publishes it to {@code <topic>-dlt} with the {@code kafka_dlt-*} headers.
 */
@TweaksSpringTest
@EmbeddedKafka(partitions = 1, topics = {DeadLetterTest.TOPIC, DeadLetterTest.TOPIC + "-dlt"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class DeadLetterTest {

    static final String TOPIC = "test.errors";

    @TestConfiguration(proxyBeanMethods = false)
    static class FailingListener {
        final AtomicInteger attempts = new AtomicInteger();

        @Bean
        DefaultErrorHandler errorHandler(KafkaTemplate<?, ?> template) {
            // 1 attempt + 2 retries without a pause, then recover: publish to test.errors-dlt (same partition).
            return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(0, 2));
        }

        @KafkaListener(id = "test-failing", groupId = "test-failing", topics = TOPIC)
        void on(String value) {
            attempts.incrementAndGet();
            throw new IllegalStateException("cannot process " + value);
        }
    }

    @Autowired
    KafkaTemplate<String, String> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    FailingListener listener;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Test
    void afterThreeAttemptsTheRecordIsInTheDeadLetterTopicWithTheReason() throws Exception {
        MessageListenerContainer container = registry.getListenerContainer("test-failing");
        container.start();
        ContainerTestUtils.waitForAssignment(container, 1);

        template.send(TOPIC, "k", "poison").get(10, TimeUnit.SECONDS);

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-dlt-reader", false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);   // consumerProps() assumes Integer keys
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            broker.consumeFromAnEmbeddedTopic(consumer, TOPIC + "-dlt");
            ConsumerRecord<String, String> dead = KafkaTestUtils.getSingleRecord(consumer, TOPIC + "-dlt", Duration.ofSeconds(30));

            assertThat(dead.key()).isEqualTo("k");
            assertThat(dead.value()).isEqualTo("poison");
            assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPIC);
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN)).endsWith("ListenerExecutionFailedException");   // Spring's wrapper
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(IllegalStateException.class.getName());
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("cannot process poison");
        }
        assertThat(listener.attempts).hasValue(3);
        container.stop();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
