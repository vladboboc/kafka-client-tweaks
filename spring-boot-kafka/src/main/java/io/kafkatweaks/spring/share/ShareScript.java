package io.kafkatweaks.spring.share;

import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.errors.recipe.TransientFailure;
import io.kafkatweaks.spring.share.recipe.RenewWhileWorking;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * Chapter 20's script and measurement, called from the recipe share listeners ({@code recipe/ShareListeners}): which
 * record is poison, which one fails once, which one is slow, which acknowledgement is "forgotten", and every delivery's
 * {@code deliveryCount()}, so the tables can show what came back a second time.
 */
@Component
@Profile("spring-share")
public class ShareScript {

    /** The one record of the lock topic whose handler takes {@link #SLOW_MS}. */
    public static final long SLOW_OFFSET = 20;
    public static final long SLOW_MS = 3000;
    /** spring.queue-0 at this offset is never acknowledged by the MANUAL listener (part 2). */
    public static final long FORGOTTEN_OFFSET = 20;

    /** What the MANUAL listener is told to do with a record. */
    public enum Decision { ACCEPT, RELEASE, REJECT, NOTHING }

    /** Per listener: calls, distinct records, delivery counts, and which consumer thread saw what. */
    public static final class Stats {
        private final AtomicLong calls = new AtomicLong();
        private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        private final Map<String, AtomicLong> callsPerThread = new ConcurrentHashMap<>();
        private final Map<String, Set<Integer>> partitionsPerThread = new ConcurrentHashMap<>();
        private final AtomicInteger maxDeliveryCount = new AtomicInteger();
        private final AtomicLong redelivered = new AtomicLong();
        private volatile long firstCallNanos;

        public long calls() {
            return calls.get();
        }

        /** Milliseconds from {@code sinceNanos} to the first record the listener saw (a share member gets its assignment through heartbeats). */
        public double firstRecordMs(long sinceNanos) {
            return firstCallNanos == 0 ? Double.NaN : (firstCallNanos - sinceNanos) / 1_000_000.0;
        }

        public int distinct() {
            return deliveries.size();
        }

        public int maxDeliveryCount() {
            return maxDeliveryCount.get();
        }

        /** Deliveries whose {@code deliveryCount()} was above 1. */
        public long redelivered() {
            return redelivered.get();
        }

        public Map<String, Long> callsPerThread() {
            var sorted = new TreeMap<String, Long>();
            callsPerThread.forEach((thread, n) -> sorted.put(thread, n.get()));
            return sorted;
        }

        public String partitions(String thread) {
            return partitionsPerThread.getOrDefault(thread, Set.of()).stream().sorted().map(String::valueOf).collect(Collectors.joining(","));
        }
    }

    private final Map<String, Stats> stats = new ConcurrentHashMap<>();
    private volatile long workMs = 1;

    // part 2
    private final AtomicLong manualAcknowledged = new AtomicLong();
    private final AtomicLong manualReleased = new AtomicLong();
    private final AtomicLong manualRejected = new AtomicLong();
    private final AtomicLong manualForgotten = new AtomicLong();
    private final Set<String> manualTerminal = ConcurrentHashMap.newKeySet();
    private volatile String stalledThread = "?";

    // part 3
    private final AtomicLong recoverOk = new AtomicLong();
    private final AtomicLong transientThrown = new AtomicLong();
    private final AtomicLong poisonThrown = new AtomicLong();

    // part 4
    private final AtomicLong renewals = new AtomicLong();
    private final AtomicLong redeliveredWhileRenewing = new AtomicLong();
    private final AtomicBoolean renewDone = new AtomicBoolean();

    public Stats stats(String listenerId) {
        return stats.computeIfAbsent(listenerId, k -> new Stats());
    }

    public void workMs(long ms) {
        this.workMs = ms;
    }

    /** Records one delivery; returns its deliveryCount (1 on the first delivery; the broker counts). */
    public int track(String listenerId, ConsumerRecord<?, ?> record) {
        Stats s = stats(listenerId);
        if (s.calls.incrementAndGet() == 1) {
            s.firstCallNanos = System.nanoTime();
        }
        int delivery = record.deliveryCount().map(Short::intValue).orElse(1);
        s.deliveries.computeIfAbsent(record.partition() + "-" + record.offset(), k -> new AtomicInteger()).incrementAndGet();
        s.maxDeliveryCount.accumulateAndGet(delivery, Math::max);
        if (delivery > 1) {
            s.redelivered.incrementAndGet();
        }
        String thread = Thread.currentThread().getName();
        s.callsPerThread.computeIfAbsent(thread, k -> new AtomicLong()).incrementAndGet();
        s.partitionsPerThread.computeIfAbsent(thread, k -> ConcurrentHashMap.newKeySet()).add(record.partition());
        return delivery;
    }

    // ---- part 1 ---------------------------------------------------------------------------------------------------

    /**
     * The simulated work of part 1. Without any work per record the first two consumers to fetch drain the topic in a
     * few ms, before the other members' first share fetch even reaches the broker.
     */
    public void work(String listenerId, ConsumerRecord<?, ?> record) {
        track(listenerId, record);
        LockSupport.parkNanos(workMs * 1_000_000);
    }

    // ---- part 2 ---------------------------------------------------------------------------------------------------

    /** Every 10th record (offset ending in 3) is poison, offsets ending in 7 are "not now" once, one record is forgotten. */
    public Decision manualDecision(ConsumerRecord<?, ?> record) {
        int delivery = track("share-manual", record);
        String id = record.partition() + "-" + record.offset();
        if (record.offset() % 10 == 3) {
            manualRejected.incrementAndGet();
            manualTerminal.add(id);
            return Decision.REJECT;
        }
        if (record.offset() % 10 == 7 && delivery == 1) {
            manualReleased.incrementAndGet();
            return Decision.RELEASE;
        }
        if (record.partition() == 0 && record.offset() == FORGOTTEN_OFFSET && delivery == 1) {
            manualForgotten.incrementAndGet();
            stalledThread = Thread.currentThread().getName();
            return Decision.NOTHING;
        }
        manualAcknowledged.incrementAndGet();
        manualTerminal.add(id);
        return Decision.ACCEPT;
    }

    // ---- part 3 ---------------------------------------------------------------------------------------------------

    /** Offsets ending in 5 fail once with a TransientFailure (the recoverer RELEASEs), offsets ending in 9 always fail (REJECT). */
    public void workOrFail(String listenerId, ConsumerRecord<String, String> record) {
        int delivery = track(listenerId, record);
        if (record.offset() % 10 == 5 && delivery == 1) {
            transientThrown.incrementAndGet();
            throw new TransientFailure("downstream timeout for " + record.key());
        }
        if (record.offset() % 10 == 9) {
            poisonThrown.incrementAndGet();
            throw new IllegalStateException("cannot process " + record.key());
        }
        recoverOk.incrementAndGet();
    }

    // ---- part 4 ---------------------------------------------------------------------------------------------------

    public boolean isSlow(ConsumerRecord<?, ?> record) {
        return record.offset() == SLOW_OFFSET;
    }

    /** The slow record's work, on the consumer thread (parts 4a/4b) or on a worker (4c). */
    public void slowWork() {
        DemoSupport.sleep(SLOW_MS);
    }

    public void renewStep(RenewWhileWorking.Step step) {
        switch (step) {
            case STARTED -> renewals.incrementAndGet();
            case STILL_WORKING -> {
                redeliveredWhileRenewing.incrementAndGet();   // the renewed record, back from poll(): still being worked on
                renewals.incrementAndGet();
            }
            case DONE -> renewDone.set(true);
        }
    }

    // ---- counters for the demo ------------------------------------------------------------------------------------

    public long manualAcknowledged() {
        return manualAcknowledged.get();
    }

    public long manualReleased() {
        return manualReleased.get();
    }

    public long manualRejected() {
        return manualRejected.get();
    }

    public long manualForgotten() {
        return manualForgotten.get();
    }

    /** Distinct records that got a terminal decision (acknowledge or reject) from the MANUAL listener. */
    public long manualTerminal() {
        return manualTerminal.size();
    }

    public String stalledThread() {
        return stalledThread;
    }

    public long recoverOk() {
        return recoverOk.get();
    }

    public long transientThrown() {
        return transientThrown.get();
    }

    public long poisonThrown() {
        return poisonThrown.get();
    }

    public long renewals() {
        return renewals.get();
    }

    public long redeliveredWhileRenewing() {
        return redeliveredWhileRenewing.get();
    }

    public boolean renewDone() {
        return renewDone.get();
    }
}
