package io.kafkatweaks.spring.parallel.recipe;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Chapter 17 · Slow a listener down from the outside: when a downstream system is overloaded, or during maintenance.
 * {@code pause()} takes effect before the next {@code poll()} (records already fetched are still delivered; set
 * {@code pauseImmediate=true} on the container to stop after the current record). The consumer keeps polling, so it
 * stays in the group and keeps its partitions; {@code resume()} continues from the position it had. Measured: no record
 * processed during 2 s of pause, delivery continued after resume.
 */
@Component
@Profile("spring-concurrency")
public class BackPressure {

    private final KafkaListenerEndpointRegistry registry;

    public BackPressure(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pause(String listenerId) {
        container(listenerId).pause();
    }

    public void resume(String listenerId) {
        container(listenerId).resume();
    }

    /** True once the consumer has actually paused (pause() only requests it). */
    public boolean isPaused(String listenerId) {
        return container(listenerId).isContainerPaused();
    }

    private MessageListenerContainer container(String listenerId) {
        return registry.getListenerContainer(listenerId);
    }
}
