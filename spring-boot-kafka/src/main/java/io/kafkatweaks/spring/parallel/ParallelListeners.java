package io.kafkatweaks.spring.parallel;

import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The listeners of chapter 17, all on the 6-partition {@code spring.parallel} topic, each in its own group.
 * Every record costs {@code workMs} of simulated work ({@code LockSupport.parkNanos}), so the drain times the demo
 * prints are decided by how many records are worked on at the same time.
 */
@Component
@Profile("spring-concurrency")
public class ParallelListeners implements DisposableBean {

    public static final List<String> SCALING = List.of("par-1", "par-3", "par-6", "par-8");

    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> threadNames = new ConcurrentHashMap<>();
    private final Map<Integer, ExecutorService> partitionWorkers = new ConcurrentHashMap<>();
    private final AtomicLong batchCalls = new AtomicLong();
    private final AtomicInteger largestBatch = new AtomicInteger();
    private volatile boolean virtualThreads;
    private volatile long workMs = 1;

    public void workMs(long ms) {
        this.workMs = ms;
    }

    public long count(String listenerId) {
        return counts.getOrDefault(listenerId, new AtomicLong()).get();
    }

    public Set<String> threads(String listenerId) {
        return Set.copyOf(threadNames.getOrDefault(listenerId, Set.of()));
    }

    public boolean virtualThreads() {
        return virtualThreads;
    }

    public long batchCalls() {
        return batchCalls.get();
    }

    public int largestBatch() {
        return largestBatch.get();
    }

    private void work() {
        LockSupport.parkNanos(workMs * 1_000_000);
    }

    private void done(String listenerId, long records) {
        counts.computeIfAbsent(listenerId, k -> new AtomicLong()).addAndGet(records);
        Thread thread = Thread.currentThread();
        threadNames.computeIfAbsent(listenerId, k -> ConcurrentHashMap.newKeySet()).add(thread.getName());
        virtualThreads = thread.isVirtual();
    }

    // ---- 1. the same handler, 1 / 3 / 6 / 8 consumers ---------------------------------------------------------

    @KafkaListener(id = "par-1", groupId = "spring-par-1", clientIdPrefix = "par-1", topics = TopicsConfig.PARALLEL, concurrency = "1")
    public void one(ConsumerRecord<String, String> record) {
        work();
        done("par-1", 1);
    }

    @KafkaListener(id = "par-3", groupId = "spring-par-3", clientIdPrefix = "par-3", topics = TopicsConfig.PARALLEL, concurrency = "3")
    public void three(ConsumerRecord<String, String> record) {
        work();
        done("par-3", 1);
    }

    @KafkaListener(id = "par-6", groupId = "spring-par-6", clientIdPrefix = "par-6", topics = TopicsConfig.PARALLEL, concurrency = "6")
    public void six(ConsumerRecord<String, String> record) {
        work();
        done("par-6", 1);
    }

    @KafkaListener(id = "par-8", groupId = "spring-par-8", clientIdPrefix = "par-8", topics = TopicsConfig.PARALLEL, concurrency = "8")
    public void eight(ConsumerRecord<String, String> record) {
        work();
        done("par-8", 1);
    }

    // ---- 2. a batch listener: the whole poll per call; concurrency comes from spring.kafka.listener.concurrency ----

    @KafkaListener(id = "par-batch", groupId = "spring-par-batch", clientIdPrefix = "par-batch", topics = TopicsConfig.PARALLEL, batch = "true")
    public void batch(List<ConsumerRecord<String, String>> records) {
        // The cost model of a bulk write: one round trip per CALL (2 ms), not one per record.
        LockSupport.parkNanos(2_000_000);
        batchCalls.incrementAndGet();
        largestBatch.accumulateAndGet(records.size(), Math::max);
        done("par-batch", records.size());
    }

    // ---- 3. one consumer thread, six virtual workers, acknowledgements out of order (asyncAcks) -----------------

    @KafkaListener(id = "par-async", groupId = "spring-par-async", clientIdPrefix = "par-async", topics = TopicsConfig.PARALLEL,
            concurrency = "1", ackMode = "MANUAL", containerFactory = "asyncAckContainerFactory",
            properties = "max.partition.fetch.bytes:16384")   // small per-partition fetches => a poll mixes all partitions (chapter 10)
    public void async(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // Chapter 10's per-partition workers: one virtual thread per partition keeps the order within the partition;
        // acknowledge() may be called from that thread because the container was told to expect out-of-order acks.
        partitionWorkers.computeIfAbsent(record.partition(),
                        p -> Executors.newSingleThreadExecutor(Thread.ofVirtual().name("worker-p" + p).factory()))
                .execute(() -> {
                    work();
                    // Acknowledge FIRST, then count: done() is what the demo's await watches, so counting first
                    // lets it stop the container while this record's acknowledgement is still outstanding - and
                    // with asyncAcks the container is paused waiting for exactly that acknowledgement.
                    ack.acknowledge();
                    done("par-async", 1);
                });
    }

    /** Also run when the context closes, so a demo that fails half way still leaves no worker behind. Idempotent. */
    @Override
    public void destroy() {
        shutdownWorkers();
    }

    public void shutdownWorkers() {
        partitionWorkers.values().forEach(ExecutorService::shutdown);
    }

    // ---- 4. paused and resumed from the outside ---------------------------------------------------------------

    @KafkaListener(id = "par-pause", groupId = "spring-par-pause", clientIdPrefix = "par-pause", topics = TopicsConfig.PARALLEL, concurrency = "1")
    public void pausable(ConsumerRecord<String, String> record) {
        work();
        done("par-pause", 1);
    }
}
