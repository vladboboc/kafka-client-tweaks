package io.kafkatweaks.spring.listener.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 16 · A {@link RecordInterceptor}: runs on the consumer thread before the listener method, for every record.
 * Return the record (possibly changed) to continue, or {@code null} to skip the listener for it. Tracing, MDC context,
 * audit counters; here: records per consumer group.
 */
public final class CountingRecordInterceptor implements RecordInterceptor<Object, Object> {

    private final Map<String, AtomicLong> perGroup = new ConcurrentHashMap<>();

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        perGroup.computeIfAbsent(consumer.groupMetadata().groupId(), k -> new AtomicLong()).incrementAndGet();
        return record;   // returning null would skip the listener for this record
    }

    public long count(String groupId) {
        return perGroup.getOrDefault(groupId, new AtomicLong()).get();
    }

    /** Group id -> records intercepted, sorted by group. */
    public Map<String, Long> counts() {
        var sorted = new TreeMap<String, Long>();
        perGroup.forEach((group, n) -> sorted.put(group, n.get()));
        return sorted;
    }
}
