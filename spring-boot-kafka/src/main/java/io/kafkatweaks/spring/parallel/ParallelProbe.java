package io.kafkatweaks.spring.parallel;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Chapter 17's measurement and simulated work, called from the recipe listeners: every record costs {@code workMs}
 * of {@code LockSupport.parkNanos}, so the drain times the demo prints are decided by how many records are worked on
 * at the same time. Counts records, calls and threads per listener.
 */
@Component
@Profile("spring-concurrency")
public class ParallelProbe {

    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> threadNames = new ConcurrentHashMap<>();
    private final AtomicLong batchCalls = new AtomicLong();
    private final AtomicInteger largestBatch = new AtomicInteger();
    private volatile boolean virtualThreads;
    private volatile long workMs = 1;

    public void workMs(long ms) {
        this.workMs = ms;
    }

    /** The simulated per-record work. */
    public void work() {
        LockSupport.parkNanos(workMs * 1_000_000);
    }

    /** The simulated bulk write of a batch listener: one round trip (2 ms) per CALL, not per record. */
    public void bulkWrite(int records) {
        LockSupport.parkNanos(2_000_000);
        batchCalls.incrementAndGet();
        largestBatch.accumulateAndGet(records, Math::max);
    }

    /** {@code records} records are done for this listener (the demo's awaits watch these counts). */
    public void done(String listenerId, long records) {
        counts.computeIfAbsent(listenerId, k -> new AtomicLong()).addAndGet(records);
        Thread thread = Thread.currentThread();
        threadNames.computeIfAbsent(listenerId, k -> ConcurrentHashMap.newKeySet()).add(thread.getName());
        virtualThreads = thread.isVirtual();
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
}
