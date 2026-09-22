package io.kafkatweaks.spring.share;

import io.kafkatweaks.spring.TweaksSpringTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chapter 20 on the embedded broker: a one-node KRaft cluster formatted by spring-kafka-test has share groups
 * enabled (Kafka 4.2 formats new clusters with {@code share.version=1}), so a share listener can be tested without
 * the Docker stack, once the share coordinator's internal topic fits on one broker (see the broker properties).
 * The group is told to start at {@code earliest}; without that, records sent before the member has its assignment
 * (up to one heartbeat interval, 5 s) would never be delivered to it.
 */
@TweaksSpringTest
@EmbeddedKafka(partitions = 1, topics = ShareListenerTest.TOPIC, bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        // The share coordinator keeps its state in __share_group_state, replication factor 3 by default; spring-kafka-test
        // lowers the offsets topic's factor to the broker count but not this one, and a one-node cluster cannot create it.
        brokerProperties = {"share.coordinator.state.topic.replication.factor=1", "share.coordinator.state.topic.min.isr=1"})
@DirtiesContext
class ShareListenerTest {

    static final String TOPIC = "test.queue";
    static final String GROUP = "test-share";

    /**
     * The factories live in their own configuration class: a {@code @KafkaListener} that names its
     * {@code containerFactory} is resolved while its own bean is being created, so the factory must not be a
     * {@code @Bean} method of the same class (circular reference otherwise).
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class ShareFactories {

        @Bean
        ShareConsumerFactory<String, String> testShareConsumerFactory(KafkaProperties properties) {
            Map<String, Object> configs = new HashMap<>(properties.buildConsumerProperties());
            configs.keySet().removeAll(ShareConfig.CONSUMER_GROUP_ONLY);
            return new DefaultShareConsumerFactory<>(configs);
        }

        @Bean
        ShareKafkaListenerContainerFactory<String, String> testShareContainerFactory(ShareConsumerFactory<String, String> factory) {
            var containerFactory = new ShareKafkaListenerContainerFactory<>(factory);
            containerFactory.setAutoStartup(false);
            return containerFactory;
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ShareBeans {
        final CountDownLatch latch = new CountDownLatch(3);
        final List<String> received = new CopyOnWriteArrayList<>();

        @KafkaListener(id = "test-share", groupId = GROUP, topics = TOPIC, containerFactory = "testShareContainerFactory")
        void on(ConsumerRecord<String, String> record) {
            received.add(record.value());
            latch.countDown();
        }
    }

    @Autowired
    KafkaTemplate<String, String> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    ShareBeans beans;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Test
    void aShareListenerReceivesRecordsFromTheEmbeddedBroker() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            var group = new ConfigResource(ConfigResource.Type.GROUP, GROUP);
            admin.incrementalAlterConfigs(Map.of(group, List.of(
                    new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET)))).all().get(10, TimeUnit.SECONDS);
        }
        var container = registry.getListenerContainer("test-share");
        container.start();   // no partition assignment to wait for: a share consumer gets records, not partitions

        for (int i = 1; i <= 3; i++) {
            template.send(TOPIC, "k" + i, "job-" + i).get(10, TimeUnit.SECONDS);
        }

        assertThat(beans.latch.await(60, TimeUnit.SECONDS)).as("share listener received all records").isTrue();
        assertThat(beans.received).containsExactlyInAnyOrder("job-1", "job-2", "job-3");
        container.stop();
    }
}
