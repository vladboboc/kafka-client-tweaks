package io.kafkatweaks.spring.errors.recipe;

import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Chapter 18 · Blocking retries with back-off, exception classification, and a dead-letter topic. The poison-pill half
 * (a value that cannot be deserialized) is {@code ErrorHandlingDeserializer} in application-spring-error-handling.yml;
 * the non-blocking alternative is the {@code @RetryableTopic} listener in {@link OrderListeners}.
 * <p>
 * Measured by the {@code spring-error-handling} demo (docs/18-spring-error-handling-retry.md): the flaky record was
 * tried at 1016, 1520, 2344 and 3155 ms (the 200 / 400 / 800 ms back-offs, blocking its partition), then dead-lettered
 * with its exception in the {@code kafka_dlt-*} headers; the not-retryable one went to the DLT after one attempt.
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-error-handling")
public class ErrorHandlingRecipe {

    /**
     * The default factory plus an error handler of our own. Boot's configurer applies spring.kafka.listener.*; then:
     * retry 3 times with 200 / 400 / 800 ms in between, never retry IllegalArgumentException, and hand what is left to
     * the dead-letter publisher. The handler is deliberately NOT a bean: Boot would wire a {@code CommonErrorHandler}
     * bean into the default factory, and the {@code @RetryableTopic} listener must keep the retry-topic infrastructure's
     * own handler.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> blockingRetryFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory, DltPublisher dlt) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        var backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(200);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(2000);
        // DeadLetterPublishingRecoverer defaults: destination "<topic>-dlt", same partition as the original record.
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(dlt.template()), backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(handler);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);   // KafkaHeaders.DELIVERY_ATTEMPT on every delivery
        return factory;
    }
}
