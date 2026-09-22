package io.kafkatweaks.spring.parallel;

import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listener containers publish Spring application events; any {@code @EventListener} bean can react. The demo
 * records pause/resume events per listener and counts the idle events {@code spring.kafka.listener.idle-event-interval} enables.
 */
@Component
@Profile("spring-concurrency")
public class ContainerEvents {

    public record Event(String listenerId, String description) {
    }

    private final List<Event> pauseResume = new CopyOnWriteArrayList<>();
    private final AtomicLong idleEvents = new AtomicLong();

    @EventListener
    public void on(ConsumerPausedEvent event) {
        pauseResume.add(new Event(listenerId(event), "ConsumerPausedEvent " + event.getPartitions()));
    }

    @EventListener
    public void on(ConsumerResumedEvent event) {
        pauseResume.add(new Event(listenerId(event), "ConsumerResumedEvent " + event.getPartitions()));
    }

    @EventListener
    public void on(ListenerContainerIdleEvent event) {
        idleEvents.incrementAndGet();
    }

    /** Pause/resume events of the listener whose id starts with the prefix (child containers are "<id>-0", "<id>-1", ...). */
    public List<String> pauseResume(String listenerIdPrefix) {
        return pauseResume.stream().filter(e -> e.listenerId().startsWith(listenerIdPrefix)).map(Event::description).toList();
    }

    public long pauseResumeCount() {
        return pauseResume.size();
    }

    public long idleEvents() {
        return idleEvents.get();
    }

    private static String listenerId(KafkaEvent event) {
        MessageListenerContainer container = event.getContainer(MessageListenerContainer.class);
        return container == null || container.getListenerId() == null ? "?" : container.getListenerId();
    }
}
