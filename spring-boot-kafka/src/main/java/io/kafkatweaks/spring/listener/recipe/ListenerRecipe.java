package io.kafkatweaks.spring.listener.recipe;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

/**
 * Chapter 16 · Two beans that sit in front of the listener method: a filter picked by name, and a global interceptor.
 * The ack modes themselves are one attribute per listener ({@link AckModeListeners}) plus the factory defaults in
 * application-spring-listener-acks.yml.
 * <p>
 * Boot wires a {@code RecordFilterStrategy} or {@code RecordInterceptor} bean into its default container factory only
 * when the bean is typed {@code <Object, Object>} (and unique). That generic type is the switch between "global" and
 * "only where named".
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-listener-acks")
public class ListenerRecipe {

    /**
     * Referenced by name from {@code @KafkaListener(filter = "oddOffsetFilter")}. Typed {@code <String, String>}, so Boot
     * does NOT wire it into every container. Discarded records are still part of the poll: their offsets are committed.
     */
    @Bean
    RecordFilterStrategy<String, String> oddOffsetFilter() {
        return record -> record.offset() % 2 == 1;   // true = discard
    }

    /** A {@code RecordInterceptor<Object, Object>}: Boot wires it into the default factory, so it runs before EVERY listener. */
    @Bean
    CountingRecordInterceptor countingInterceptor() {
        return new CountingRecordInterceptor();
    }
}
