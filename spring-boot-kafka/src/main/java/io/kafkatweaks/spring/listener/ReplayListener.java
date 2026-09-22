package io.kafkatweaks.spring.listener;

import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 16, part 3: replay with {@link ConsumerSeekAware}. The container hands the listener a seek callback
 * when partitions are assigned; seeking there overrides whatever offset the group had committed (the Spring
 * spelling of chapter 08's {@code seek*} calls). {@code seekRelative(-100)} = "the last 100 records of every
 * partition"; {@code seekToTimestamp}, {@code seekToBeginning}, {@code seekToEnd} and absolute {@code seek} exist too.
 */
@Component
@Profile("spring-listener-acks")
public class ReplayListener implements ConsumerSeekAware {

    private final AtomicLong replayed = new AtomicLong();
    private final Map<Integer, Long> firstOffsets = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> callback.seekRelative(tp.topic(), tp.partition(), -100, false));
    }

    @KafkaListener(id = "acks-replay", groupId = "spring-acks-replay", clientIdPrefix = "acks-replay", topics = TopicsConfig.LISTENER, ackMode = "MANUAL")
    public void replay(ConsumerRecord<String, String> record, Acknowledgment ack) {
        replayed.incrementAndGet();
        firstOffsets.putIfAbsent(record.partition(), record.offset());
        // MANUAL without acknowledge(): a replay window that never moves the group's committed offsets.
    }

    public long replayed() {
        return replayed.get();
    }

    public Map<Integer, Long> firstOffsets() {
        return new TreeMap<>(firstOffsets);
    }
}
