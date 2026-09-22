package io.kafkatweaks.spring.parallel.recipe;

import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Chapter 17 · A second container factory for a container-level setting. Everything else of this chapter is
 * application-spring-concurrency.yml (virtual threads, default concurrency, batch type, idle events) and the
 * attributes of {@link ParallelListeners}.
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-concurrency")
public class ConcurrencyRecipe {

    /**
     * Boot's configurer applies everything {@code spring.kafka.listener.*} says (auto-startup, poll timeout, the
     * virtual-thread executor ...), then the factory is changed where the default one cannot be. Listeners pick it with
     * {@code containerFactory = "asyncAckContainerFactory"}. (asyncAcks happens to have a Boot key,
     * spring.kafka.listener.async-acks; deliveryAttemptHeader, pauseImmediate or micrometerTags do not, and this is how
     * you set those.)
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> asyncAckContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        // Acknowledgements may arrive in any order; the container commits contiguous prefixes and pauses the
        // consumer until every record of the poll is acknowledged. Requires ackMode MANUAL or MANUAL_IMMEDIATE.
        factory.getContainerProperties().setAsyncAcks(true);
        return factory;
    }
}
