package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CommitOnRevokeTest {

    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    private final List<String> calls = new ArrayList<>();
    private final CommitOnRevoke listener = new CommitOnRevoke(consumer, new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            calls.add("revoked " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            calls.add("assigned " + partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            calls.add("lost " + partitions);
        }
    });

    @Test
    void revokedPartitionsAreCommittedUpToTheLastDoneRecordBeforeTheDelegateRuns() {
        consumer.assign(List.of(P0, P1));
        listener.markDone(record(P0, 4));
        listener.markDone(record(P1, 9));

        listener.onPartitionsRevoked(List.of(P0));

        assertThat(consumer.committed(Set.of(P0, P1))).containsOnlyKeys(P0);
        assertThat(consumer.committed(Set.of(P0)).get(P0).offset()).isEqualTo(5);
        assertThat(calls).containsExactly("revoked [t-0]");
    }

    @Test
    void lostPartitionsAreNeverCommitted() {
        consumer.assign(List.of(P0));
        listener.markDone(record(P0, 4));

        listener.onPartitionsLost(List.of(P0));
        listener.commitDone();   // nothing left to commit for the lost partition

        assertThat(consumer.committed(Set.of(P0))).isEmpty();
        assertThat(calls).containsExactly("lost [t-0]");
    }

    @Test
    void commitDoneCommitsEverythingMarkedAndClears() {
        consumer.assign(List.of(P0, P1));
        listener.markDone(record(P0, 1));
        listener.markDone(record(P0, 2));
        listener.markDone(record(P1, 0));

        listener.commitDone();
        listener.onPartitionsRevoked(List.of(P0, P1));   // already committed: no second commit, just the delegate

        assertThat(consumer.committed(Set.of(P0, P1))).containsExactlyInAnyOrderEntriesOf(Map.of(
                P0, new org.apache.kafka.clients.consumer.OffsetAndMetadata(3),
                P1, new org.apache.kafka.clients.consumer.OffsetAndMetadata(1)));
        assertThat(calls).containsExactly("revoked [t-0, t-1]");
    }

    private static ConsumerRecord<String, String> record(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "k", "v");
    }
}
