package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 10 · A slow handler without more partitions: one consumer, one sequential worker per partition.
 * <p>
 * The poll thread never does the work and never waits for it. Every partition gets its own single-threaded worker
 * (on a virtual thread), so records of one partition stay in order. {@code pause()}/{@code resume()} bound the work in
 * flight, and only offsets whose records are DONE are committed: per partition, "everything below this watermark".
 * Measured by the {@code consumer-parallel} demo (docs/10-consumer-parallel.md), 6 000 records of 5 ms work, 6 partitions:
 * <pre>
 *   one consumer, sequential                      175 records/s
 *   six consumers, one per partition              649 records/s
 *   one PartitionedWorkerConsumer                 628 records/s, paused twice, ordering per partition kept
 * </pre>
 * <pre>{@code
 * props.putAll(PartitionedWorkerConsumer.config());
 * try (var consumer = new KafkaConsumer<String, String>(props);
 *      var pipeline = new PartitionedWorkerConsumer<>(consumer, record -> callSlowService(record), 3000, 1500)) {
 *     pipeline.subscribe(List.of("orders"));
 *     while (running) {
 *         pipeline.pollOnce(Duration.ofMillis(100));
 *     }
 * }   // closes the pipeline first: finishes the queued work, then commits the watermarks
 * }</pre>
 * Not thread-safe: call everything from the thread that owns the consumer.
 */
public final class PartitionedWorkerConsumer<K, V> implements AutoCloseable {

    /** Your processing, run on the partition's worker. Throwing stops that partition at this record. */
    @FunctionalInterface
    public interface RecordHandler<K, V> {
        void handle(ConsumerRecord<K, V> record) throws Exception;
    }

    /** What the pipeline needs from the consumer configuration. */
    public static Map<String, Object> config() {
        return Map.of(
                // The offsets come from the watermarks. Auto-commit would commit the POSITION: fetched, not done.
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                // poll() hands back records partition by partition, draining up to this much of one partition first
                // (default 1 MB = thousands of small records). Small per-partition fetches make every poll span many
                // partitions, so every worker gets something to do.
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 16 * 1024);
    }

    private record Failure(long offset, Exception error) {
    }

    private final Consumer<K, V> consumer;
    private final RecordHandler<K, V> handler;
    private final int pauseAbove;
    private final int resumeBelow;
    private final Map<TopicPartition, ExecutorService> workers = new HashMap<>();              // poll thread only
    private final Map<TopicPartition, AtomicLong> nextToCommit = new ConcurrentHashMap<>();    // written by the workers
    private final Map<TopicPartition, Failure> failed = new ConcurrentHashMap<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private boolean paused;
    private int pauses;

    /**
     * @param pauseAbove  stop fetching when more records than this are queued or running in the workers
     * @param resumeBelow start fetching again when fewer than this are left
     */
    public PartitionedWorkerConsumer(Consumer<K, V> consumer, RecordHandler<K, V> handler, int pauseAbove, int resumeBelow) {
        this.consumer = consumer;
        this.handler = handler;
        this.pauseAbove = pauseAbove;
        this.resumeBelow = resumeBelow;
    }

    /** Subscribe through the pipeline: partitions that move away are finished and committed first. */
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Finish what was handed out for them (inside poll(): keep that well below max.poll.interval.ms),
                // commit how far they got, forget them.
                var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (TopicPartition partition : partitions) {
                    ExecutorService worker = workers.remove(partition);
                    if (worker != null) {
                        worker.close();   // waits for the partition's queue
                    }
                    AtomicLong next = nextToCommit.remove(partition);
                    if (next != null) {
                        offsets.put(partition, new OffsetAndMetadata(next.get()));
                    }
                    failed.remove(partition);
                }
                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                // Someone else owns them already: drop the queued work and commit nothing.
                for (TopicPartition partition : partitions) {
                    ExecutorService worker = workers.remove(partition);
                    if (worker != null) {
                        inFlight.addAndGet(-worker.shutdownNow().size());   // queued tasks never run, so they never count down
                        worker.close();
                    }
                    nextToCommit.remove(partition);
                    failed.remove(partition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (paused) {
                    consumer.pause(partitions);   // new partitions arrive un-paused
                }
            }
        });
    }

    /**
     * One turn of the loop: poll, hand every record to its partition's worker, apply back-pressure, commit the
     * watermarks asynchronously. Returns at once; the work happens on the workers.
     *
     * @return the number of records this poll returned
     * @throws IllegalStateException when a handler failed earlier: its partition stopped at that record. Close the
     *                               pipeline (it commits everything before the failed record) and decide what to do.
     */
    public int pollOnce(Duration timeout) {
        if (!failed.isEmpty()) {
            var first = failed.entrySet().iterator().next();
            throw new IllegalStateException("%s@%d failed, the partition stopped there".formatted(first.getKey(), first.getValue().offset()),
                    first.getValue().error());
        }
        ConsumerRecords<K, V> batch = consumer.poll(timeout);
        for (ConsumerRecord<K, V> record : batch) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            inFlight.incrementAndGet();
            // One single-threaded executor per partition: FIFO per partition, so order is kept.
            workers.computeIfAbsent(partition, p -> Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()))
                    .submit(() -> process(partition, record));
        }
        // Back-pressure: stop fetching while the workers are behind, but keep calling poll() so the group still sees
        // this member alive (max.poll.interval.ms).
        if (!paused && inFlight.get() > pauseAbove) {
            consumer.pause(consumer.assignment());
            paused = true;
            pauses++;
        } else if (paused && inFlight.get() < resumeBelow) {
            consumer.resume(consumer.assignment());
            paused = false;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = watermarks();
        if (!offsets.isEmpty()) {
            consumer.commitAsync(offsets, null);   // a lost async commit is harmless: the next one carries a higher watermark
        }
        return batch.count();
    }

    private void process(TopicPartition partition, ConsumerRecord<K, V> record) {
        try {
            if (failed.containsKey(partition)) {
                return;   // an earlier record of this partition failed: go no further, or the order would break
            }
            handler.handle(record);
            // Only this partition's worker writes this entry, one record at a time: the watermark only ever grows.
            nextToCommit.computeIfAbsent(partition, p -> new AtomicLong()).set(record.offset() + 1);
        } catch (Exception e) {
            failed.putIfAbsent(partition, new Failure(record.offset(), e));
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /** Per partition, the offset below which every record is done: the only offsets that are safe to commit. */
    public Map<TopicPartition, OffsetAndMetadata> watermarks() {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        nextToCommit.forEach((partition, next) -> offsets.put(partition, new OffsetAndMetadata(next.get())));
        return offsets;
    }

    /** How often back-pressure paused fetching. */
    public int pauses() {
        return pauses;
    }

    /**
     * Finish the queued work, then commit the watermarks synchronously. Never the no-arg {@code commitSync()}: it
     * commits the consumer's position, i.e. everything fetched, including records still waiting in a worker's queue.
     */
    @Override
    public void close() {
        workers.values().forEach(ExecutorService::close);
        workers.clear();
        Map<TopicPartition, OffsetAndMetadata> offsets = watermarks();
        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
        }
    }
}
