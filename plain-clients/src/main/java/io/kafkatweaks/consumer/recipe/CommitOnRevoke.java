package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Chapter 09 · Commit what you finished before a partition moves, so its next owner does not do it again.
 * <p>
 * The rebalance callbacks run on the poll thread, inside {@code poll()}. {@code onPartitionsRevoked} is the last moment
 * this member owns the partitions, so it commits their progress there. {@code onPartitionsLost} (the session expired,
 * someone else owns them already) must NOT commit, and the interface's default implementation of it calls
 * {@code onPartitionsRevoked}, so it is overridden here.
 * <pre>{@code
 * var rebalance = new CommitOnRevoke(consumer);
 * consumer.subscribe(List.of("orders"), rebalance);
 * try {
 *     while (running.get()) {
 *         for (var record : consumer.poll(Duration.ofMillis(500))) {
 *             handle(record);
 *             rebalance.markDone(record);
 *         }
 *         rebalance.commitDone();
 *     }
 * } catch (WakeupException e) {
 *     // another thread set running=false and called consumer.wakeup() to interrupt a blocking poll()
 * } finally {
 *     rebalance.commitDone();
 *     consumer.close();   // leaves the group at once (a static member keeps its partitions until the session timeout)
 * }
 * }</pre>
 * Not thread-safe: {@link #markDone} and {@link #commitDone} belong on the poll thread, like the callbacks.
 */
public final class CommitOnRevoke implements ConsumerRebalanceListener {

    private final Consumer<?, ?> consumer;
    private final ConsumerRebalanceListener delegate;
    private final Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();

    public CommitOnRevoke(Consumer<?, ?> consumer) {
        this(consumer, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });
    }

    /** @param delegate your own listener (logging, releasing per-partition state), called after the commit */
    public CommitOnRevoke(Consumer<?, ?> consumer, ConsumerRebalanceListener delegate) {
        this.consumer = consumer;
        this.delegate = delegate;
    }

    /** The record is fully processed: its partition may be committed up to it. */
    public void markDone(ConsumerRecord<?, ?> record) {
        done.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }

    /** Commit everything marked so far, e.g. once per poll. */
    public void commitDone() {
        if (!done.isEmpty()) {
            consumer.commitSync(done);
            done.clear();
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        var revoked = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offset = done.remove(partition);
            if (offset != null) {
                revoked.put(partition, offset);
            }
        }
        if (!revoked.isEmpty()) {
            consumer.commitSync(revoked);   // synchronous: the partition must not move before its progress is stored
        }
        delegate.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        partitions.forEach(done::remove);   // too late: another member may own them already, a commit would fail or undo its progress
        delegate.onPartitionsLost(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        delegate.onPartitionsAssigned(partitions);
    }
}
