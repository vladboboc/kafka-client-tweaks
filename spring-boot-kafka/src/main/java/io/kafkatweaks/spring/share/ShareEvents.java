package io.kafkatweaks.spring.share;

import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.event.ShareConsumerStoppingEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The lifecycle events a share container publishes: the usual {@code ConsumerStartedEvent} / {@code ConsumerStoppedEvent}
 * per consumer thread, plus {@link ShareConsumerStoppingEvent} (carrying the {@code ShareConsumer}) right before each
 * one is closed. There are no rebalance, idle or pause events: a share consumer owns no partitions.
 */
@Component
@Profile("spring-share")
public class ShareEvents {

    private final Map<String, Map<String, AtomicInteger>> counts = new ConcurrentHashMap<>();

    @EventListener
    public void on(ConsumerStartedEvent event) {
        count(event, "ConsumerStartedEvent");
    }

    @EventListener
    public void on(ShareConsumerStoppingEvent event) {
        count(event, "ShareConsumerStoppingEvent");
    }

    @EventListener
    public void on(ConsumerStoppedEvent event) {
        count(event, "ConsumerStoppedEvent(" + event.getReason() + ")");
    }

    @EventListener
    public void on(ConsumerFailedToStartEvent event) {
        count(event, "ConsumerFailedToStartEvent");
    }

    private void count(KafkaEvent event, String what) {
        MessageListenerContainer container = event.getContainer(MessageListenerContainer.class);
        String id = container == null || container.getListenerId() == null ? "?" : container.getListenerId();
        counts.computeIfAbsent(id, k -> new ConcurrentHashMap<>()).computeIfAbsent(what, k -> new AtomicInteger()).incrementAndGet();
    }

    /** {@code "ConsumerStartedEvent x4, ShareConsumerStoppingEvent x4, ..."} for one listener. */
    public String summary(String listenerId) {
        return new TreeMap<>(counts.getOrDefault(listenerId, Map.of())).entrySet().stream()
                .map(e -> e.getKey() + " x" + e.getValue().get())
                .collect(Collectors.joining(", "));
    }
}
