package io.kafkatweaks.spring.listener;

import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The listeners of chapter 16. Every one reads the whole {@code spring.listener} topic in its own group; they
 * differ only in the {@code ackMode} attribute (spring-kafka 4.1), so the commit counts the demo prints are the
 * effect of that attribute alone. All containers start stopped ({@code spring.kafka.listener.auto-startup=false})
 * and the demo starts them one by one.
 */
@Component
@Profile("spring-listener-acks")
public class AckModeListeners {

    /** listener id -> what its ackMode means; the ids are also the {@code clientIdPrefix} values. */
    public static final Map<String, String> ACK_MODES = Map.of(
            "acks-record", "RECORD: commit after every record",
            "acks-batch", "BATCH: commit after the records of a poll were processed",
            "acks-time", "TIME: like BATCH, but only if ack-time (1s) passed since the last commit",
            "acks-count", "COUNT: like BATCH, but only once ack-count (1000) records were processed",
            "acks-manual", "MANUAL_IMMEDIATE: commit when the listener calls acknowledge() (here: every 500th record)");
    public static final List<String> ACK_MODE_LISTENERS = List.of("acks-record", "acks-batch", "acks-time", "acks-count", "acks-manual");

    public record Delivery(long tMs, int partition, long offset, int attempt, String action) {
    }

    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<Long, Integer> nackAttempts = new ConcurrentHashMap<>();
    private final List<Delivery> nackTimeline = new CopyOnWriteArrayList<>();
    private volatile long nackStartedNanos;

    public long count(String listenerId) {
        return counts.getOrDefault(listenerId, new AtomicLong()).get();
    }

    private long increment(String listenerId) {
        return counts.computeIfAbsent(listenerId, k -> new AtomicLong()).incrementAndGet();
    }

    // ---- 1. one listener per ack mode ---------------------------------------------------------------------

    @KafkaListener(id = "acks-record", groupId = "spring-acks-record", clientIdPrefix = "acks-record", topics = TopicsConfig.LISTENER, ackMode = "RECORD")
    public void record(ConsumerRecord<String, String> record) {
        increment("acks-record");
    }

    @KafkaListener(id = "acks-batch", groupId = "spring-acks-batch", clientIdPrefix = "acks-batch", topics = TopicsConfig.LISTENER, ackMode = "BATCH")
    public void batch(ConsumerRecord<String, String> record) {
        increment("acks-batch");
    }

    @KafkaListener(id = "acks-time", groupId = "spring-acks-time", clientIdPrefix = "acks-time", topics = TopicsConfig.LISTENER, ackMode = "TIME")
    public void time(ConsumerRecord<String, String> record) {
        increment("acks-time");
    }

    @KafkaListener(id = "acks-count", groupId = "spring-acks-count", clientIdPrefix = "acks-count", topics = TopicsConfig.LISTENER, ackMode = "COUNT")
    public void count(ConsumerRecord<String, String> record) {
        increment("acks-count");
    }

    @KafkaListener(id = "acks-manual", groupId = "spring-acks-manual", clientIdPrefix = "acks-manual", topics = TopicsConfig.LISTENER, ackMode = "MANUAL_IMMEDIATE")
    public void manual(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (increment("acks-manual") % 500 == 0) {
            ack.acknowledge();   // commits the offset of THIS record immediately (MANUAL would defer to the end of the poll)
        }
    }

    // ---- 2. nack: manual assignment of partition 0 from offset 0, small polls, one negative acknowledgement ---

    @KafkaListener(id = "acks-nack", groupId = "spring-acks-nack", clientIdPrefix = "acks-nack", ackMode = "MANUAL",
            properties = "max.poll.records:5",
            topicPartitions = @TopicPartition(topic = TopicsConfig.LISTENER,
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")))
    public void nack(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (nackStartedNanos == 0) {
            nackStartedNanos = System.nanoTime();
        }
        int attempt = nackAttempts.merge(record.offset(), 1, Integer::sum);
        boolean reject = record.offset() == 3 && attempt == 1;
        nackTimeline.add(new Delivery((System.nanoTime() - nackStartedNanos) / 1_000_000, record.partition(), record.offset(), attempt,
                reject ? "nack(1s)" : "acknowledge()"));
        if (reject) {
            ack.nack(Duration.ofSeconds(1));   // commit what was acked, drop the rest of this poll, seek back to this record, pause 1 s
        } else {
            ack.acknowledge();
        }
    }

    public List<Delivery> nackTimeline() {
        return List.copyOf(nackTimeline);
    }

    // ---- 4. a filtered listener (the RecordFilterStrategy bean is picked by name via the filter attribute) ----

    @KafkaListener(id = "acks-filter", groupId = "spring-acks-filter", clientIdPrefix = "acks-filter", topics = TopicsConfig.LISTENER, filter = "oddOffsetFilter")
    public void filtered(ConsumerRecord<String, String> record) {
        increment("acks-filter");
    }
}
