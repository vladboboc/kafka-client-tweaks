package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Chapter 06 · Transactions: several writes as one, and exactly-once consume-transform-produce.
 * <p>
 * The output records and the input offsets are committed in ONE transaction, so they become visible together or not
 * at all. Measured by the {@code producer-transactions} demo (docs/06-producer-transactions.md): 2 000 input records,
 * a crash in the middle of a transaction, a restart with the same {@code transactional.id}, and the output holds
 * exactly 2 000 records with no key seen twice.
 * <pre>{@code
 * var producer = new KafkaProducer<String, String>(ExactlyOnceProcessor.transactional(props, "orders-enricher-0", Duration.ofSeconds(30)));
 * var consumer = new KafkaConsumer<String, String>(ExactlyOnceProcessor.readCommitted(consumerProps, "orders-enricher"));
 * producer.initTransactions();          // fences the previous instance with this id, aborts what it left open
 * consumer.subscribe(List.of("orders"));
 * while (running) {
 *     var batch = consumer.poll(Duration.ofMillis(500));
 *     if (!batch.isEmpty()) {
 *         ExactlyOnceProcessor.processBatch(consumer, producer, batch, r -> new ProducerRecord<>("orders-enriched", r.key(), enrich(r.value())));
 *     }
 * }
 * }</pre>
 * Only Kafka to Kafka is exactly-once. A side effect outside Kafka (a database, an HTTP call) needs chapter 08's
 * idempotent handler.
 */
public final class ExactlyOnceProcessor {

    private ExactlyOnceProcessor() {
    }

    /**
     * A transactional producer. The id must be STABLE across restarts of the same logical instance (e.g. the service
     * name plus a shard number): a new instance with the same id fences the old one, which is how a zombie is stopped.
     * Setting it turns on idempotence and {@code acks=all}.
     */
    public static Properties transactional(Properties producerProps, String transactionalId, Duration transactionTimeout) {
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        // The coordinator aborts a transaction that stays open longer than this (default 60 s). It is also how long an
        // open transaction of a crashed instance can hold back read_committed consumers if no successor fences it.
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) transactionTimeout.toMillis());
        return producerProps;
    }

    /** The input side: skip aborted data, and never commit offsets outside the transaction. */
    public static Properties readCommitted(Properties consumerProps, String groupId) {
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Default read_uncommitted returns aborted records and records of still-open transactions.
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // Offsets travel inside the producer's transaction (sendOffsetsToTransaction), not through the consumer.
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return consumerProps;
    }

    /** Records to any number of partitions, all visible or none. The producer must have called initTransactions(). */
    public static <K, V> void sendAtomically(Producer<K, V> producer, List<ProducerRecord<K, V>> records) {
        producer.beginTransaction();
        try {
            records.forEach(producer::send);
            producer.commitTransaction();   // flushes, then the coordinator writes a commit marker on every partition touched
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException fatal) {
            throw fatal;   // this producer is finished: close it (another instance owns the id now, or it is not allowed)
        } catch (KafkaException e) {
            producer.abortTransaction();   // read_committed consumers will never see these records
            throw e;
        }
    }

    /**
     * One poll's worth of input as one transaction: transform every record, send the results, and commit the input
     * offsets as part of the same transaction. One transaction per poll keeps the commit cost negligible: a commit is
     * a handful of round trips (~40-50 ms on the demo stack), so a transaction per record ran at 21 records/s.
     * <p>
     * A non-Kafka exception from {@code transform} (a bug) propagates with the transaction still open. Close the
     * producer: the coordinator aborts the transaction after {@code transaction.timeout.ms}, or at once when a successor
     * with the same id calls {@code initTransactions()}. That is exactly what a crash looks like, and it loses and
     * duplicates nothing.
     *
     * @return true when committed; false when the transaction was aborted and the consumer rewound to the start of the
     *         batch, so the next poll returns the same records again
     */
    public static <K, V, K2, V2> boolean processBatch(Consumer<K, V> consumer, Producer<K2, V2> producer, ConsumerRecords<K, V> batch,
                                                      Function<ConsumerRecord<K, V>, ProducerRecord<K2, V2>> transform) {
        producer.beginTransaction();
        try {
            for (ConsumerRecord<K, V> record : batch) {
                producer.send(transform.apply(record));
            }
            // The input offsets become part of the transaction: committed together with the output, or never.
            producer.sendOffsetsToTransaction(nextOffsets(batch), consumer.groupMetadata());
            producer.commitTransaction();
            return true;
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException fatal) {
            throw fatal;   // another instance took over this transactional.id: close this producer and stop
        } catch (KafkaException e) {
            producer.abortTransaction();   // the output of this batch is discarded ...
            rewind(consumer, batch);       // ... and the batch is read again
            return false;
        }
    }

    /**
     * The offset to commit per partition is the NEXT one to read: last processed + 1. Kafka 4.0's
     * {@code batch.nextOffsets()} returns the same, but an EMPTY map when an interceptor rebuilt the batch with the
     * deprecated {@code ConsumerRecords(Map)} constructor, and committing an empty map silently commits nothing.
     */
    private static Map<TopicPartition, OffsetAndMetadata> nextOffsets(ConsumerRecords<?, ?> batch) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : batch.partitions()) {
            offsets.put(partition, new OffsetAndMetadata(batch.records(partition).getLast().offset() + 1));
        }
        return offsets;
    }

    private static void rewind(Consumer<?, ?> consumer, ConsumerRecords<?, ?> batch) {
        for (TopicPartition partition : batch.partitions()) {
            consumer.seek(partition, batch.records(partition).getFirst().offset());
        }
    }
}
