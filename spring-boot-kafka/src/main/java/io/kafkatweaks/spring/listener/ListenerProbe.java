package io.kafkatweaks.spring.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 16's measurement, called from the recipe listeners where a real listener would do its work: record counts
 * per listener, the nack script and its timeline, and what the replay listener saw.
 */
@Component
@Profile("spring-listener-acks")
public class ListenerProbe {

    public record Delivery(long tMs, int partition, long offset, int attempt, String action) {
    }

    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<Long, Integer> nackAttempts = new ConcurrentHashMap<>();
    private final List<Delivery> nackTimeline = new CopyOnWriteArrayList<>();
    private volatile long nackStartedNanos;
    private final AtomicLong replayed = new AtomicLong();
    private final Map<Integer, Long> firstReplayedOffsets = new ConcurrentHashMap<>();

    /** One record processed by the listener with this id. */
    public void hit(String listenerId) {
        counts.computeIfAbsent(listenerId, k -> new AtomicLong()).incrementAndGet();
    }

    /** Same, returning the listener's running count. */
    public long hitAndCount(String listenerId) {
        return counts.computeIfAbsent(listenerId, k -> new AtomicLong()).incrementAndGet();
    }

    public long count(String listenerId) {
        return counts.getOrDefault(listenerId, new AtomicLong()).get();
    }

    /** The script of part 2: nack offset 3 on its first delivery, acknowledge everything else. Records the timeline. */
    public boolean nackOnce(ConsumerRecord<?, ?> record) {
        if (nackStartedNanos == 0) {
            nackStartedNanos = System.nanoTime();
        }
        int attempt = nackAttempts.merge(record.offset(), 1, Integer::sum);
        boolean nack = record.offset() == 3 && attempt == 1;
        nackTimeline.add(new Delivery((System.nanoTime() - nackStartedNanos) / 1_000_000, record.partition(), record.offset(), attempt,
                nack ? "nack(1s)" : "acknowledge()"));
        return nack;
    }

    public List<Delivery> nackTimeline() {
        return List.copyOf(nackTimeline);
    }

    /** A record the replay listener received. */
    public void replayed(ConsumerRecord<?, ?> record) {
        replayed.incrementAndGet();
        firstReplayedOffsets.putIfAbsent(record.partition(), record.offset());
    }

    public long replayed() {
        return replayed.get();
    }

    public Map<Integer, Long> firstReplayedOffsets() {
        return new TreeMap<>(firstReplayedOffsets);
    }
}
