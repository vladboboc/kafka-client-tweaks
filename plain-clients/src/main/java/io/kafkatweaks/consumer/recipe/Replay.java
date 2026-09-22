package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Chapter 08 · Offsets are just numbers: replay from a point in time, re-read the last N records, or rewind a group.
 * <p>
 * All of these act on the partitions the consumer currently owns ({@code assignment()}), so call them after the
 * assignment arrived: from {@code ConsumerRebalanceListener.onPartitionsAssigned}, or after the first {@code poll()}.
 * Measured by the {@code consumer-offsets} demo (docs/08-consumer-offsets.md) on a 3 000-record topic:
 * {@code fromTime(one hour ago)} re-reads 3 000, {@code lastRecords(100)} re-reads 300 (3 partitions x 100).
 */
public final class Replay {

    private Replay() {
    }

    /**
     * Every owned partition to the first record whose timestamp is at or after {@code time}. {@code offsetsForTimes}
     * returns null for a partition with nothing that recent: that partition goes to its end.
     */
    public static void fromTime(Consumer<?, ?> consumer, Instant time) {
        Set<TopicPartition> partitions = consumer.assignment();
        var query = new HashMap<TopicPartition, Long>();
        partitions.forEach(tp -> query.put(tp, time.toEpochMilli()));
        Map<TopicPartition, OffsetAndTimestamp> found = consumer.offsetsForTimes(query);
        Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
        found.forEach((tp, offset) -> consumer.seek(tp, offset == null ? end.get(tp) : offset.offset()));
    }

    /** The last {@code n} records of every owned partition (fewer where the partition holds fewer). */
    public static void lastRecords(Consumer<?, ?> consumer, long n) {
        Set<TopicPartition> partitions = consumer.assignment();
        Map<TopicPartition, Long> begin = consumer.beginningOffsets(partitions);
        consumer.endOffsets(partitions).forEach((tp, end) -> consumer.seek(tp, Math.max(begin.get(tp), end - n)));
    }

    /**
     * Rewind the whole GROUP, not just this consumer: commit the beginning of every owned partition, so whoever owns
     * them next (this instance after a restart, or another member after a rebalance) starts from there. From the
     * outside, with the group stopped: {@code kafka-consumer-groups --reset-offsets --to-earliest --execute}.
     */
    public static void rewindGroup(Consumer<?, ?> consumer) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        consumer.beginningOffsets(consumer.assignment()).forEach((tp, begin) -> offsets.put(tp, new OffsetAndMetadata(begin)));
        consumer.commitSync(offsets);
    }
}
