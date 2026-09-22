package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionedWorkerConsumerTest {

    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");

    @Test
    void recordsOfOnePartitionAreHandledInOrderAndOnlyDoneOffsetsAreCommitted() throws Exception {
        seed(3);
        var order = new ConcurrentHashMap<Integer, List<Long>>();
        var pipeline = new PartitionedWorkerConsumer<>(consumer,
                r -> order.computeIfAbsent(r.partition(), p -> new CopyOnWriteArrayList<>()).add(r.offset()), 100, 50);

        pipeline.pollOnce(Duration.ZERO);
        pipeline.close();   // drains the workers, then commits the watermarks

        assertThat(order.get(0)).containsExactly(0L, 1L, 2L);
        assertThat(order.get(1)).containsExactly(0L, 1L, 2L);
        assertThat(consumer.committed(Set.of(P0, P1))).containsExactlyInAnyOrderEntriesOf(Map.of(
                P0, new OffsetAndMetadata(3), P1, new OffsetAndMetadata(3)));
    }

    @Test
    void aFailedRecordStopsItsPartitionThereAndALaterPollReportsIt() throws Exception {
        seed(3);
        var handlerCalls = new CopyOnWriteArrayList<String>();
        var pipeline = new PartitionedWorkerConsumer<String, String>(consumer, r -> {
            handlerCalls.add(r.partition() + "@" + r.offset());
            if (r.partition() == 0 && r.offset() == 1) {
                throw new IllegalStateException("bad record");
            }
        }, 100, 50);

        pipeline.pollOnce(Duration.ZERO);
        awaitWatermark(pipeline, P1, 3);

        // The workers run asynchronously: keep polling until the failure surfaces.
        assertThatThrownBy(() -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                pipeline.pollOnce(Duration.ZERO);
                Thread.sleep(5);
            }
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("t-0@1").hasRootCauseMessage("bad record");
        assertThat(pipeline.watermarks()).containsEntry(P0, new OffsetAndMetadata(1));   // p0@0 done, p0@1 failed
        assertThat(handlerCalls).doesNotContain("0@2");   // nothing after the failed record, or the order would break
    }

    @Test
    void aRevokedPartitionIsFinishedAndCommittedBeforeItMoves() {
        var pipeline = new PartitionedWorkerConsumer<String, String>(consumer, r -> { }, 100, 50);
        pipeline.subscribe(List.of("t"));
        consumer.rebalance(List.of(P0, P1));
        seed(2);

        pipeline.pollOnce(Duration.ZERO);
        consumer.rebalance(List.of(P1));   // P0 moves away: the listener drains it and commits
        assertThat(pipeline.watermarks()).doesNotContainKey(P0);

        consumer.rebalance(List.of(P0, P1));   // MockConsumer only reports real committed offsets for assigned partitions
        assertThat(consumer.committed(Set.of(P0)).get(P0)).isEqualTo(new OffsetAndMetadata(2));
    }

    private void seed(int perPartition) {
        if (consumer.assignment().isEmpty()) {
            consumer.assign(List.of(P0, P1));
        }
        consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
        for (int i = 0; i < perPartition; i++) {
            consumer.addRecord(new ConsumerRecord<>("t", 0, i, "k", "v"));
            consumer.addRecord(new ConsumerRecord<>("t", 1, i, "k", "v"));
        }
    }

    private static void awaitWatermark(PartitionedWorkerConsumer<?, ?> pipeline, TopicPartition tp, long offset) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!new OffsetAndMetadata(offset).equals(pipeline.watermarks().get(tp)) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }
}
