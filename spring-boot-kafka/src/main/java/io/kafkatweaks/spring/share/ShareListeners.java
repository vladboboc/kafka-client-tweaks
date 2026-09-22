package io.kafkatweaks.spring.share;

import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.errors.TransientFailure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * The share listeners of chapter 20. A share {@code @KafkaListener} looks like any other, except for the
 * {@code containerFactory} (a {@link org.springframework.kafka.config.ShareKafkaListenerContainerFactory}) and, in
 * MANUAL mode, the {@link ShareAcknowledgment} parameter. Two things the annotation offers are ignored by the share
 * container in 4.1: {@code clientIdPrefix} (the client.id is the listener id, plus {@code -n} per consumer thread) and
 * {@code properties} (client overrides belong on the share consumer factory).
 * <p>
 * Every record's {@code deliveryCount()} is tracked, so the tables can show what came back a second time.
 */
@Component
@Profile("spring-share")
public class ShareListeners implements DisposableBean {

    /** One partition, so that both consumer threads of a lock demo are necessarily fed from the same partition. */
    public static final String LOCKS_TOPIC = "spring.queue-locks";
    /** The one record of the lock topic whose handler takes {@link #SLOW_MS}. */
    public static final long SLOW_OFFSET = 20;
    public static final long SLOW_MS = 3000;
    /** spring.queue-0 at this offset is never acknowledged by the MANUAL listener (part 2). */
    public static final long FORGOTTEN_OFFSET = 20;

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
    private final ExecutorService worker = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("slow-worker-", 0).factory());
    private final Map<Long, Future<?>> slowJobs = new ConcurrentHashMap<>();
    private final AtomicLong renewals = new AtomicLong();
    private final AtomicLong redeliveredWhileRenewing = new AtomicLong();
    private final AtomicBoolean renewDone = new AtomicBoolean();

    public Stats stats(String listenerId) {
        return stats.computeIfAbsent(listenerId, k -> new Stats());
    }

    private int track(String listenerId, ConsumerRecord<?, ?> record) {
        Stats s = stats(listenerId);
        if (s.calls.incrementAndGet() == 1) {
            s.firstCallNanos = System.nanoTime();
        }
        int delivery = record.deliveryCount().map(Short::intValue).orElse(1);   // 1 on the first delivery; the broker counts
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

    // ---- 1. EXPLICIT (the default): return = ACCEPT, sent by the container ------------------------------------------

    @KafkaListener(id = "share-explicit", groupId = "spring-share-explicit", topics = TopicsConfig.QUEUE,
            containerFactory = "shareKafkaListenerContainerFactory", concurrency = "4")
    public void explicit(ConsumerRecord<String, String> record) {
        track("share-explicit", record);
        // Without any work per record the first two consumers to fetch drain the topic in a few ms, before the other
        // members' first share fetch even reaches the broker.
        LockSupport.parkNanos(workMs * 1_000_000);
    }

    public void workMs(long ms) {
        this.workMs = ms;
    }

    // ---- 2. MANUAL: the listener decides per record, and MUST decide -------------------------------------------------

    @KafkaListener(id = "share-manual", groupId = "spring-share-manual", topics = TopicsConfig.QUEUE,
            containerFactory = "manualShareContainerFactory", concurrency = "2")
    public void manual(ConsumerRecord<String, String> record, ShareAcknowledgment ack) {
        int delivery = track("share-manual", record);
        String id = record.partition() + "-" + record.offset();
        if (record.offset() % 10 == 3) {
            ack.reject();                                  // poison: archived, never delivered again
            manualRejected.incrementAndGet();
            manualTerminal.add(id);
        } else if (record.offset() % 10 == 7 && delivery == 1) {
            ack.release();                                 // "not now": back to the queue, deliveryCount + 1, any member may get it
            manualReleased.incrementAndGet();
        } else if (record.partition() == 0 && record.offset() == FORGOTTEN_OFFSET && delivery == 1) {
            // The bug this mode makes possible: no acknowledgement at all. The consumer thread that delivered this record
            // cannot poll again (the client refuses to poll with unacknowledged records), so its whole poll is never committed.
            manualForgotten.incrementAndGet();
            stalledThread = Thread.currentThread().getName();
        } else {
            ack.acknowledge();                             // ACCEPT
            manualAcknowledged.incrementAndGet();
            manualTerminal.add(id);
        }
    }

    // ---- 3. EXPLICIT + a recoverer: exceptions become RELEASE or REJECT ------------------------------------------------

    @KafkaListener(id = "share-recover", groupId = "spring-share-recover", topics = TopicsConfig.QUEUE,
            containerFactory = "recoveringShareContainerFactory", concurrency = "2")
    public void recovering(ConsumerRecord<String, String> record) {
        int delivery = track("share-recover", record);
        if (record.offset() % 10 == 5 && delivery == 1) {
            transientThrown.incrementAndGet();
            throw new TransientFailure("downstream timeout for " + record.key());   // recoverer: RELEASE
        }
        if (record.offset() % 10 == 9) {
            poisonThrown.incrementAndGet();
            throw new IllegalStateException("cannot process " + record.key());      // recoverer: REJECT
        }
        recoverOk.incrementAndGet();
    }

    // ---- 4. acquisition locks: one slow record, three ways ---------------------------------------------------------------

    @KafkaListener(id = "share-lock-2s", groupId = "spring-share-lock-2s", topics = LOCKS_TOPIC,
            containerFactory = "shareKafkaListenerContainerFactory")
    public void lock2s(ConsumerRecord<String, String> record) {
        slow("share-lock-2s", record);
    }

    @KafkaListener(id = "share-lock-10s", groupId = "spring-share-lock-10s", topics = LOCKS_TOPIC,
            containerFactory = "shareKafkaListenerContainerFactory")
    public void lock10s(ConsumerRecord<String, String> record) {
        slow("share-lock-10s", record);
    }

    private void slow(String listenerId, ConsumerRecord<String, String> record) {
        track(listenerId, record);
        if (record.offset() == SLOW_OFFSET) {
            DemoSupport.sleep(SLOW_MS);   // on the consumer thread: the poll's other records wait for their commit too
        }
    }

    /**
     * MANUAL mode with the work on another thread and {@code renew()} while it runs, the way the KafkaShareConsumer
     * javadoc prescribes it. A renewal is an acknowledgement type: the client sends it with the next poll, the broker
     * extends the lock, and the record comes back from that poll (same delivery, same {@code deliveryCount}), so the
     * listener sees the slow record again about once per container poll (1 s) while the worker is busy with it.
     * Every decision (renew again, or acknowledge because the work is done) is taken right there, on the consumer
     * thread, with the acknowledgment of the current delivery. The worker never touches an acknowledgment: between
     * a renewal being sent and the record coming back it is not in flight, and an acknowledgement queued from another
     * thread in that window fails with "The record cannot be acknowledged" (a lesson of this demo's first version).
     */
    @KafkaListener(id = "share-lock-renew", groupId = "spring-share-lock-renew", topics = LOCKS_TOPIC,
            containerFactory = "manualShareContainerFactory")
    public void lockRenew(ConsumerRecord<String, String> record, ShareAcknowledgment ack) {
        track("share-lock-renew", record);
        if (record.offset() != SLOW_OFFSET) {
            ack.acknowledge();
            return;
        }
        Future<?> job = slowJobs.get(record.offset());
        if (job == null) {
            // First delivery: hand the 3 s of work to a worker and keep the 2 s lock alive.
            slowJobs.put(record.offset(), worker.submit(() -> DemoSupport.sleep(SLOW_MS)));
            ack.renew();
            renewals.incrementAndGet();
        } else if (job.isDone()) {
            slowJobs.remove(record.offset());
            ack.acknowledge();
            renewDone.set(true);
        } else {
            redeliveredWhileRenewing.incrementAndGet();   // the renewed record, back from poll(): still being worked on
            ack.renew();
            renewals.incrementAndGet();
        }
    }

    /** Also run when the context closes, so a demo that fails half way still leaves no worker behind. Idempotent. */
    @Override
    public void destroy() {
        shutdown();
    }

    public void shutdown() {
        worker.shutdown();
    }

    // ---- counters for the demo ------------------------------------------------------------------------------------------

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
