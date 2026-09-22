package io.kafkatweaks.consumer.recipe;

import io.kafkatweaks.consumer.recipe.AtLeastOnceConsumer.CommitPoint;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AtLeastOnceConsumerTest {

    private static final TopicPartition TP = new TopicPartition("t", 0);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");

    @BeforeEach
    void threeRecords() {
        consumer.assign(List.of(TP));
        consumer.updateBeginningOffsets(Map.of(TP, 0L));
        for (int i = 0; i < 3; i++) {
            consumer.addRecord(new ConsumerRecord<>("t", 0, i, "k" + i, "v" + i));
        }
    }

    @Test
    void afterHandlingCommitsThePositionPastTheBatch() throws Exception {
        var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.AFTER_HANDLING, r -> { });

        assertThat(loop.pollOnce(Duration.ZERO)).isEqualTo(3);
        assertThat(committed()).isEqualTo(3);
    }

    @Test
    void aFailingHandlerCommitsNothingOfTheBatch() {
        var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.AFTER_HANDLING, r -> {
            if (r.offset() == 1) {
                throw new IllegalStateException("downstream is down");
            }
        });

        assertThatThrownBy(() -> loop.pollOnce(Duration.ZERO)).hasMessage("downstream is down");
        assertThat(consumer.committed(Set.of(TP)).get(TP)).isNull();
    }

    @Test
    void beforeHandlingHasCommittedTheBatchEvenWhenTheHandlerFails() {
        var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.BEFORE_HANDLING, r -> {
            throw new IllegalStateException("crash");
        });

        assertThatThrownBy(() -> loop.pollOnce(Duration.ZERO)).hasMessage("crash");
        assertThat(committed()).isEqualTo(3);   // at-most-once: the unhandled records count as done
    }

    @Test
    void skipDuplicatesHandsOnlyUnseenIdsToTheHandler() throws Exception {
        Set<String> done = new HashSet<>(Set.of("t-0-1"));
        List<Long> handled = new ArrayList<>();
        var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.AFTER_HANDLING, AtLeastOnceConsumer.skipDuplicates(
                r -> r.topic() + "-" + r.partition() + "-" + r.offset(), done::contains, r -> handled.add(r.offset())));

        loop.pollOnce(Duration.ZERO);

        assertThat(handled).containsExactly(0L, 2L);
    }

    @Test
    void anEmptyPollCommitsNothing() throws Exception {
        consumer.poll(Duration.ZERO);   // drain the three records
        var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.AFTER_HANDLING, r -> { });

        assertThat(loop.pollOnce(Duration.ZERO)).isZero();
        assertThat(consumer.committed(Set.of(TP)).get(TP)).isNull();
    }

    private long committed() {
        OffsetAndMetadata offset = consumer.committed(Set.of(TP)).get(TP);
        return offset == null ? -1 : offset.offset();
    }
}
