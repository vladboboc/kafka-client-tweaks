package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Chapter 08 · Offsets: where you commit decides what a crash costs.
 * <p>
 * The committed offset is the consumer's only durable state. Measured by the {@code consumer-offsets} demo
 * (docs/08-consumer-offsets.md): 3 000 records, a crash after record 1234 in the middle of a 500-record batch, then a
 * second instance of the group finishes the topic:
 * <pre>
 *   CommitPoint.BEFORE_HANDLING                     at-most-once       266 records never processed
 *   CommitPoint.AFTER_HANDLING                      at-least-once      234 records processed twice
 *   AFTER_HANDLING + skipDuplicates(...)            effectively-once     0 missing, 0 processed twice
 * </pre>
 * <pre>{@code
 * props.putAll(AtLeastOnceConsumer.manualCommits());
 * try (var consumer = new KafkaConsumer<String, String>(props)) {
 *     consumer.subscribe(List.of("payments"));
 *     var loop = new AtLeastOnceConsumer<>(consumer, CommitPoint.AFTER_HANDLING, record -> book(record.value()));
 *     while (running) {
 *         loop.pollOnce(Duration.ofMillis(500));
 *     }
 * }
 * }</pre>
 */
public final class AtLeastOnceConsumer<K, V> {

    /** Your processing. Throwing means "not done": nothing of the current batch gets committed. */
    @FunctionalInterface
    public interface RecordHandler<K, V> {
        void handle(ConsumerRecord<K, V> record) throws Exception;
    }

    public enum CommitPoint {
        /** At-least-once: commit once the whole batch is handled. A crash re-delivers the unfinished batch. */
        AFTER_HANDLING,
        /** At-most-once: commit first, then handle. A crash loses the rest of the batch. Only for data you may lose. */
        BEFORE_HANDLING
    }

    /** You decide when a record counts as done, not a timer. */
    public static Map<String, Object> manualCommits() {
        // Default true: every auto.commit.interval.ms (5 s) poll() commits whatever the PREVIOUS poll returned.
        // At-least-once only while all processing happens on the poll thread; hand records to another thread and
        // keep polling, and the offsets get committed before the work is done.
        return Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    private final Consumer<K, V> consumer;
    private final CommitPoint commitPoint;
    private final RecordHandler<K, V> handler;

    public AtLeastOnceConsumer(Consumer<K, V> consumer, CommitPoint commitPoint, RecordHandler<K, V> handler) {
        this.consumer = consumer;
        this.commitPoint = commitPoint;
        this.handler = handler;
    }

    /**
     * One turn of the poll loop: poll, handle every record, commit. If the handler throws, the exception propagates
     * and nothing of this batch is committed (and {@code close()} does not commit either with auto-commit off), so the
     * next owner of these partitions starts again from the last commit.
     *
     * @return the number of records this poll returned; 0 means nothing new arrived within {@code timeout}
     */
    public int pollOnce(Duration timeout) throws Exception {
        ConsumerRecords<K, V> batch = consumer.poll(timeout);
        if (batch.isEmpty()) {
            return 0;
        }
        if (commitPoint == CommitPoint.BEFORE_HANDLING) {
            consumer.commitSync();   // the position is already past this batch: this commits "we will have handled these"
        }
        for (ConsumerRecord<K, V> record : batch) {
            handler.handle(record);
        }
        if (commitPoint == CommitPoint.AFTER_HANDLING) {
            consumer.commitSync();   // commits the position after the batch: "these are done"
        }
        return batch.count();
    }

    /**
     * Effectively-once on top of at-least-once: the redelivered records still arrive, and the handler recognises what
     * it already did. {@code alreadyHandled} looks the id up in durable storage; the wrapped handler must store the id
     * in the SAME transaction as its side effect (a processed-events table, an upsert keyed by the id, the inbox
     * pattern), otherwise a crash between the two brings the duplicate back.
     *
     * @param idOf a stable id per record: an event id from the payload, or topic-partition-offset
     */
    public static <K, V> RecordHandler<K, V> skipDuplicates(Function<ConsumerRecord<K, V>, String> idOf,
                                                            Predicate<String> alreadyHandled, RecordHandler<K, V> handler) {
        return record -> {
            if (!alreadyHandled.test(idOf.apply(record))) {
                handler.handle(record);
            }
        };
    }
}
