package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 12 · A {@link ConsumerInterceptor}: runs inside {@code poll()} for every batch and on every commit. This one
 * measures produce-to-consume latency from the {@code sent-at} header of {@link StampingProducerInterceptor} and counts
 * commits. Register it with {@code ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG}.
 * <p>
 * {@code onConsume} may filter or transform the batch; if it builds a new {@code ConsumerRecords}, use the
 * two-argument constructor that keeps the next offsets, or {@code nextOffsets()} downstream returns an empty map.
 */
public final class LatencyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /** Static only because the demo reads them; a real interceptor would publish metrics instead. */
    public static final AtomicLong LATENCY_SUM = new AtomicLong();
    public static final AtomicInteger RECORDS = new AtomicInteger();
    public static final AtomicInteger COMMITS = new AtomicInteger();

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        for (var r : records) {
            Header h = r.headers().lastHeader("sent-at");
            if (h != null) {
                LATENCY_SUM.addAndGet(now - Long.parseLong(new String(h.value(), StandardCharsets.UTF_8)));
                RECORDS.incrementAndGet();
            }
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        COMMITS.incrementAndGet();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
