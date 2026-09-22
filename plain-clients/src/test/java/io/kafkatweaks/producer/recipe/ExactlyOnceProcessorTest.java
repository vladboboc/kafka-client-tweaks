package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExactlyOnceProcessorTest {

    private static final TopicPartition IN_0 = new TopicPartition("in", 0);
    private static final TopicPartition IN_1 = new TopicPartition("in", 1);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    private final MockProducer<String, String> producer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    private ConsumerRecords<String, String> batch;

    @BeforeEach
    void pollOneBatch() {
        consumer.assign(List.of(IN_0, IN_1));
        consumer.updateBeginningOffsets(Map.of(IN_0, 0L, IN_1, 0L));
        consumer.addRecord(new ConsumerRecord<>("in", 0, 0, "k0", "a"));
        consumer.addRecord(new ConsumerRecord<>("in", 0, 1, "k1", "b"));
        consumer.addRecord(new ConsumerRecord<>("in", 1, 7, "k2", "c"));
        batch = consumer.poll(Duration.ZERO);
        producer.initTransactions();
    }

    @Test
    void outputAndInputOffsetsCommitInOneTransaction() {
        boolean committed = ExactlyOnceProcessor.processBatch(consumer, producer, batch, ExactlyOnceProcessorTest::upperCase);

        assertThat(committed).isTrue();
        assertThat(producer.transactionCommitted()).isTrue();
        assertThat(producer.history()).extracting(ProducerRecord::value).containsExactlyInAnyOrder("A", "B", "C");
        // The committed offset is the NEXT record to read: last processed + 1, per partition.
        Map<TopicPartition, OffsetAndMetadata> offsets = producer.consumerGroupOffsetsHistory().getFirst().values().iterator().next();
        assertThat(offsets).containsOnly(Map.entry(IN_0, new OffsetAndMetadata(2)), Map.entry(IN_1, new OffsetAndMetadata(8)));
    }

    @Test
    void aKafkaErrorAbortsAndRewindsToTheStartOfTheBatch() {
        producer.commitTransactionException = new TimeoutException("coordinator did not answer");

        boolean committed = ExactlyOnceProcessor.processBatch(consumer, producer, batch, ExactlyOnceProcessorTest::upperCase);

        assertThat(committed).isFalse();
        assertThat(producer.transactionAborted()).isTrue();
        assertThat(consumer.position(IN_0)).isZero();
        assertThat(consumer.position(IN_1)).isEqualTo(7);
    }

    @Test
    void aFencedProducerIsNotAbortedButRethrown() {
        producer.sendOffsetsToTransactionException = new ProducerFencedException("another instance owns the id");

        assertThatThrownBy(() -> ExactlyOnceProcessor.processBatch(consumer, producer, batch, ExactlyOnceProcessorTest::upperCase))
                .isInstanceOf(ProducerFencedException.class);
        assertThat(producer.transactionAborted()).isFalse();
    }

    @Test
    void aBugInTheTransformLeavesTheTransactionOpenLikeACrash() {
        assertThatThrownBy(() -> ExactlyOnceProcessor.processBatch(consumer, producer, batch, r -> {
            throw new IllegalStateException("bug");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(producer.transactionInFlight()).isTrue();
        assertThat(producer.transactionCommitted()).isFalse();
        assertThat(producer.transactionAborted()).isFalse();
    }

    private static ProducerRecord<String, String> upperCase(ConsumerRecord<String, String> r) {
        return new ProducerRecord<>("out", r.key(), r.value().toUpperCase());
    }
}
