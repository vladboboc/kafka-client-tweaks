package io.kafkatweaks.spring.share;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Chapter 20: Boot 4.1 auto-configures nothing for share consumers, so the two beans a share {@code @KafkaListener}
 * needs are declared here: a {@link ShareConsumerFactory} (the {@code KafkaShareConsumer} equivalent of Boot's
 * consumer factory, built from the same {@code spring.kafka.consumer.*} properties) and one
 * {@link ShareKafkaListenerContainerFactory} per acknowledgement mode the demo shows.
 * <p>
 * Every container factory here has {@code autoStartup=false}: {@code spring.kafka.listener.auto-startup} only
 * reaches Boot's own {@code ConcurrentKafkaListenerContainerFactory}.
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-share")
public class ShareConfig {

    /**
     * Consumer-group settings a share consumer has no use for. Where the topic starts, and whether transactional
     * records are visible, are GROUP configs of a share group ({@code share.auto.offset.reset},
     * {@code share.isolation.level}); acknowledgements replace offset commits; there is no static membership.
     */
    public static final Set<String> CONSUMER_GROUP_ONLY = Set.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG);

    @Bean
    ShareConsumerFactory<String, String> shareConsumerFactory(KafkaProperties properties) {
        Map<String, Object> configs = new HashMap<>(properties.buildConsumerProperties());
        configs.keySet().removeAll(CONSUMER_GROUP_ONLY);   // application.yml's auto-offset-reset=earliest among them
        return new DefaultShareConsumerFactory<>(configs);
    }

    /** EXPLICIT (the default): the container ACCEPTs every record the listener returns from; failures go to the recoverer (REJECT). */
    @Bean
    ShareKafkaListenerContainerFactory<String, String> shareKafkaListenerContainerFactory(
            ShareConsumerFactory<String, String> shareConsumerFactory, ShareOutcomes outcomes) {
        var factory = new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
        factory.setAutoStartup(false);
        factory.getContainerProperties().setShareAckMode(ContainerProperties.ShareAckMode.EXPLICIT);
        factory.getContainerProperties().setAcknowledgementCommitCallback(outcomes);
        return factory;
    }

    /** MANUAL: the listener takes a {@code ShareAcknowledgment} and must acknowledge(), release() or reject() every record. */
    @Bean
    ShareKafkaListenerContainerFactory<String, String> manualShareContainerFactory(
            ShareConsumerFactory<String, String> shareConsumerFactory, ShareOutcomes outcomes) {
        var factory = new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
        factory.setAutoStartup(false);
        factory.getContainerProperties().setShareAckMode(ContainerProperties.ShareAckMode.MANUAL);
        // How long a record may stay unacknowledged before the container logs a warning (default 30 s). Nothing else
        // happens: the consumer thread cannot poll again until every record of the poll has its acknowledgement.
        factory.getContainerProperties().setShareAcknowledgmentTimeout(Duration.ofSeconds(5));
        factory.getContainerProperties().setAcknowledgementCommitCallback(outcomes);
        return factory;
    }

    /** EXPLICIT with a custom recoverer: a thrown TransientFailure becomes RELEASE instead of REJECT. */
    @Bean
    ShareKafkaListenerContainerFactory<String, String> recoveringShareContainerFactory(
            ShareConsumerFactory<String, String> shareConsumerFactory, ShareOutcomes outcomes) {
        var factory = new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
        factory.setAutoStartup(false);
        factory.getContainerProperties().setShareAckMode(ContainerProperties.ShareAckMode.EXPLICIT);
        factory.setShareConsumerRecordRecoverer(outcomes);
        factory.getContainerProperties().setAcknowledgementCommitCallback(outcomes);
        return factory;
    }
}
