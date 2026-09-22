package io.kafkatweaks.spring;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No broker anywhere: proves the shared wiring (application.yml, TopicsConfig, DemoSupport, ClientCapture) is
 * valid Spring configuration. Boot's KafkaTemplate connects lazily, KafkaAdmin is told not to create topics, and
 * without a demo profile there is no @KafkaListener to start.
 */
@TweaksSpringTest
class ContextLoadsTest {

    @Autowired
    KafkaTemplate<?, ?> template;

    @Autowired
    ProducerFactory<?, ?> producerFactory;

    @Autowired
    KafkaAdmin admin;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Test
    void bootWiresTheKafkaBeansWithoutTalkingToABroker() {
        assertThat(template).isNotNull();
        assertThat(producerFactory.getConfigurationProperties())
                .containsEntry("client.id", "spring-tweaks")
                .containsKey("bootstrap.servers");
        assertThat(admin.getConfigurationProperties()).containsKey("bootstrap.servers");
        assertThat(registry.getListenerContainerIds()).isEmpty();
    }
}
