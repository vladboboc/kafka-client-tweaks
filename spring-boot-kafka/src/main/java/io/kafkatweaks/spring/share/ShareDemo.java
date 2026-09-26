package io.kafkatweaks.spring.share;

import io.kafkatweaks.common.Seed;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.share.recipe.ShareConfig;
import io.kafkatweaks.spring.share.recipe.ShareListeners;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Chapter 20: chapter 11's share groups (Queues for Kafka) through {@code @KafkaListener}. Measures {@link ShareListeners}
 * and {@link ShareConfig} (with {@link ShareScript} scripting the outcomes); everything else in this file is measurement.
 * <ol>
 *   <li>EXPLICIT mode (the default): four consumer threads on three partitions, the container acknowledges</li>
 *   <li>MANUAL mode: acknowledge / release / reject per record, and what a forgotten acknowledgement does</li>
 *   <li>EXPLICIT mode with a ShareConsumerRecordRecoverer: exceptions become RELEASE or REJECT</li>
 *   <li>acquisition locks: one 3 s record under a 2 s lock, a 10 s lock, and renew() from a worker thread</li>
 * </ol>
 * <pre>
 *   records=3000   records seeded into spring.queue
 *   work=1         milliseconds of simulated work per record in part 1
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-share")
public class ShareDemo {

    private static final Logger log = LoggerFactory.getLogger(ShareDemo.class);

    /** The share groups of this chapter with their share.record.lock.duration.ms. */
    static final Map<String, String> GROUP_LOCK_MS = new LinkedHashMap<>();

    static {
        GROUP_LOCK_MS.put("spring-share-explicit", "2000");
        GROUP_LOCK_MS.put("spring-share-manual", "2000");
        GROUP_LOCK_MS.put("spring-share-recover", "2000");
        GROUP_LOCK_MS.put("spring-share-lock-2s", "2000");
        GROUP_LOCK_MS.put("spring-share-lock-10s", "10000");
        GROUP_LOCK_MS.put("spring-share-lock-renew", "2000");
    }

    static final int LOCK_RECORDS = 40;

    /** When a lock variant is finished, given the acknowledgements the broker committed / refused since its start. */
    @FunctionalInterface
    interface Done {
        boolean test(long committed, long refused, ShareScript.Stats stats);
    }

    @Bean
    ApplicationRunner springShare(DemoSupport support, ShareScript script, ShareListeners listeners, ShareOutcomes outcomes, ShareEvents events) {
        return support.demo("spring-share", args -> {
            long records = args.getLong("records", 3000);
            long workMs = args.getLong("work", 1);
            script.workMs(workMs);
            long total;
            try (var topics = new Topics()) {
                topics.recreate(TopicsConfig.QUEUE, 3);
                // Producer-default batches (16 KB, no linger): the broker hands out whole batches (share.acquire.mode=batch_optimized),
                // so small batches spread the records evenly over the members.
                total = Seed.ensure(topics, TopicsConfig.QUEUE, 3, records, 200, Map.of());
                topics.recreate(TopicsConfig.QUEUE_LOCKS, 1);
                Seed.ensure(topics, TopicsConfig.QUEUE_LOCKS, 1, LOCK_RECORDS, 200, Map.of());
                for (var group : GROUP_LOCK_MS.entrySet()) {
                    // A share group keeps per-record state (delivery counts, archived records); a previous run's would distort the tables.
                    topics.deleteShareGroup(group.getKey());
                    topics.alterGroupConfigs(group.getKey(), Map.of(
                            "share.auto.offset.reset", "earliest",
                            "share.record.lock.duration.ms", group.getValue(),
                            "share.delivery.count.limit", "3"));
                }
            }
            log.info("""
                    group configs (Admin.incrementalAlterConfigs on ConfigResource.Type.GROUP, for all six groups): share.auto.offset.reset=earliest,
                      share.delivery.count.limit=3, share.record.lock.duration.ms=2000 (10000 for spring-share-lock-10s)""");

            // ---- 1. EXPLICIT: the container acknowledges ------------------------------------------------------------
            var sw = Stopwatch.start();
            long startNanos = System.nanoTime();
            support.container("share-explicit").start();   // not support.start(): a share container has no partition assignment to wait for
            await(() -> script.stats("share-explicit").calls(), total, Duration.ofSeconds(90), "share-explicit");
            double ms = sw.elapsedMillis();
            long idleMembers = logMembers("spring-share-explicit", "");
            support.stop("share-explicit");
            var s1 = script.stats("share-explicit");
            var t1 = new Table("consumer thread (one KafkaShareConsumer each)", "records", "from partitions");
            s1.callsPerThread().forEach((thread, n) -> t1.row(thread, n, s1.partitions(thread)));
            t1.row("total", s1.calls(), "distinct records %d, delivered more than once %d".formatted(s1.distinct(), s1.redelivered()));
            log.info("1. share-explicit: ShareAckMode.EXPLICIT (the default), concurrency=4 on 3 partitions, {} ms of work per record: {} records in {} ms, the first one after {} ms\n{}",
                    workMs, total, Math.round(ms), Math.round(s1.firstRecordMs(startNanos)), t1);
            log.info("""
                    the container ACCEPTs every record the listener returns from and commits a poll's acknowledgements after its last record.
                      members the coordinator gave a partition: {} of 4; consumers that received records: {} of 4.
                      The assignor guarantees every PARTITION a member, not every member a partition, and within a partition the records go to whoever
                      fetches first. A 4th consumer in a consumer GROUP on 3 partitions would have received nothing, every time.""",
                    4 - idleMembers, s1.callsPerThread().size());

            // ---- 2. MANUAL: the listener decides, and must decide -----------------------------------------------------
            sw = Stopwatch.start();
            support.container("share-manual").start();
            // Every record should get a terminal decision, but one consumer thread stalls on the forgotten acknowledgement:
            // run until the count stops moving (and the stalled thread has logged its 5 s warning).
            long lastProgressNanos = System.nanoTime();
            long lastSeen = -1;
            boolean membersLogged = false;
            while (script.manualTerminal() < total && sw.elapsedMillis() < 90_000) {
                long seen = script.manualTerminal();
                if (!membersLogged && seen > 0) {
                    logMembers("spring-share-manual", "at the first record");   // the assignment can still change for a few seconds
                    membersLogged = true;
                }
                if (seen != lastSeen) {
                    lastSeen = seen;
                    lastProgressNanos = System.nanoTime();
                } else if (System.nanoTime() - lastProgressNanos > Duration.ofSeconds(4).toNanos() && sw.elapsedMillis() > 12_000) {
                    break;   // 12 s: long enough for the stalled thread's acknowledgement-timeout warning (5 s) to be logged
                }
                DemoSupport.sleep(100);
            }
            ms = sw.elapsedMillis();
            logMembers("spring-share-manual", "at the end");
            support.stop("share-manual");
            var s2 = script.stats("share-manual");
            long undelivered = total - s2.distinct();
            var t2 = new Table("the listener called", "times", "effect");
            t2.row("ack.acknowledge()", script.manualAcknowledged(), "ACCEPT: done (includes the released records on their second delivery)");
            t2.row("ack.release()", script.manualReleased(), "RELEASE: back to the queue, deliveryCount + 1, any member of the partition may get it");
            t2.row("ack.reject()", script.manualRejected(), "REJECT: archived, never delivered again");
            t2.row("nothing (the bug)", script.manualForgotten(), "consumer thread " + script.stalledThread() + " never polls again; the acknowledgements of that whole poll are never sent");
            t2.row("(deliveries with deliveryCount > 1)", s2.redelivered(), "max deliveryCount " + s2.maxDeliveryCount() + "; distinct records delivered " + s2.distinct() + " of " + total);
            t2.row("(records never delivered)", undelivered, undelivered > 0
                    ? "in the partitions only the stalled thread was assigned: no other member may fetch them, lock or no lock"
                    : "the other thread shares the stalled thread's partitions and took its records over after the 2 s lock");
            log.info("2. share-manual: ShareAckMode.MANUAL, concurrency=2: {} listener calls, {} of {} records reached a terminal state, {} ms\n{}",
                    s2.calls(), script.manualTerminal(), total, Math.round(ms), t2);
            var t2b = new Table("consumer thread", "calls", "partitions seen", "");
            s2.callsPerThread().forEach((thread, n) -> t2b.row(thread, n, s2.partitions(thread),
                    thread.equals(script.stalledThread()) ? "stalled in the poll that contained spring.queue-0@" + ShareScript.FORGOTTEN_OFFSET : ""));
            log.info("consumer threads of share-manual (thread C-n runs member n-1 of the table above)\n{}", t2b);

            // ---- 3. EXPLICIT + recoverer -----------------------------------------------------------------------------
            sw = Stopwatch.start();
            support.container("share-recover").start();
            await(() -> script.recoverOk() + outcomes.rejected(), total, Duration.ofSeconds(90), "share-recover");
            ms = sw.elapsedMillis();
            support.stop("share-recover");
            var s3 = script.stats("share-recover");
            var t3 = new Table("listener outcome", "times", "recoverer decision", "effect");
            t3.row("returned normally", script.recoverOk(), "(none: the container ACCEPTs)", "done");
            t3.row("threw TransientFailure (first delivery only)", script.transientThrown(), "RELEASE x" + outcomes.released(), "redelivered with deliveryCount 2, then processed");
            t3.row("threw IllegalStateException", script.poisonThrown(), "REJECT x" + outcomes.rejected(), "archived (the default recoverer does this for every exception)");
            log.info("3. share-recover: EXPLICIT + ShareConsumerRecordRecoverer, concurrency=2: {} calls for {} records in {} ms; redelivered {}, max deliveryCount {}\n{}",
                    s3.calls(), total, Math.round(ms), s3.redelivered(), s3.maxDeliveryCount(), t3);

            // ---- 4. acquisition locks ---------------------------------------------------------------------------------
            var t4 = new Table("listener", "ack mode", "lock", "listener calls", "distinct records", "max deliveryCount", "acks committed", "acks refused", "renewals", "ms");
            // (a) stops once the first pass was refused and the second pass has begun; stop() lets that pass finish and commit
            lockVariant(support, script, outcomes, t4, "share-lock-2s", "EXPLICIT", "2 s",
                    (committed, refused, stats) -> refused >= LOCK_RECORDS && stats.calls() > LOCK_RECORDS, () -> "-");
            lockVariant(support, script, outcomes, t4, "share-lock-10s", "EXPLICIT", "10 s",
                    (committed, refused, stats) -> committed >= LOCK_RECORDS, () -> "-");
            // (c) the broker confirms every renewal and the final acknowledgement through the callback
            lockVariant(support, script, outcomes, t4, "share-lock-renew", "MANUAL + renew()", "2 s",
                    (committed, refused, stats) -> script.renewDone() && committed >= LOCK_RECORDS + script.renewals(),
                    () -> script.renewals() + " (record came back " + script.redeliveredWhileRenewing() + "x)");
            log.info("4. {} records on a 1-partition topic, one consumer thread, the record at offset {} takes {} ms; one poll acquires all {}\n{}",
                    LOCK_RECORDS, ShareScript.SLOW_OFFSET, ShareScript.SLOW_MS, LOCK_RECORDS, t4);
            log.info("refused with: {}", outcomes.lastRefusal());
            log.info("""
                    EXPLICIT commits a poll's acknowledgements after its LAST record: one 3 s record let all 40 locks (2 s) expire, every ACCEPT of
                      the pass was refused and the whole pass came back with deliveryCount 2 (a 3rd pass would follow, then share.delivery.count.limit=3
                      archives them: processed 3 times, never acknowledged). A lock longer than the slowest POLL fixes it. So does MANUAL mode with the
                      work on a worker thread and renew(): the renewed record comes back from every poll (~1 s) with the same deliveryCount until it is
                      acknowledged; the broker confirmed each renewal (counted under acks committed).""");

            var lifecycle = new Table("listener", "lifecycle events");
            for (String id : List.of("share-explicit", "share-manual", "share-recover", "share-lock-2s", "share-lock-10s", "share-lock-renew")) {
                lifecycle.row(id, events.summary(id));
            }
            log.info("lifecycle events (a share container publishes no rebalance, idle or pause events)\n{}", lifecycle);
            listeners.shutdown();
        });
    }

    private static void lockVariant(DemoSupport support, ShareScript script, ShareOutcomes outcomes, Table table,
                                    String id, String mode, String lock, Done done, Supplier<String> renewals) {
        String topic = TopicsConfig.QUEUE_LOCKS;
        long committed0 = outcomes.committed(topic);
        long refused0 = outcomes.refused(topic);
        var sw = Stopwatch.start();
        support.container(id).start();
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (!done.test(outcomes.committed(topic) - committed0, outcomes.refused(topic) - refused0, script.stats(id)) && System.nanoTime() < deadline) {
            DemoSupport.sleep(50);
        }
        double ms = sw.elapsedMillis();
        support.stop(id);   // waits for the poll in progress (a 3 s record included)
        var s = script.stats(id);
        table.row(id, mode, lock, s.calls(), s.distinct(), s.maxDeliveryCount(),
                outcomes.committed(topic) - committed0, outcomes.refused(topic) - refused0, renewals.get(), "%.0f".formatted(ms));
    }

    /** Logs the coordinator's view of the group (Admin.describeShareGroups); returns how many members hold no partition. */
    private static long logMembers(String group, String when) throws Exception {
        try (var topics = new Topics()) {
            ShareGroupDescription description = topics.admin().describeShareGroups(List.of(group)).all().get().get(group);
            var table = new Table("member (client.id = listener id + consumer index)", "assigned partitions");
            description.members().stream().sorted(Comparator.comparing(ShareMemberDescription::clientId)).forEach(m ->
                    table.row(m.clientId(), m.assignment().topicPartitions().stream().map(tp -> String.valueOf(tp.partition())).sorted().collect(Collectors.joining(","))));
            log.info("{}\n{}", ("share group %s: state %s %s".formatted(group, description.groupState(), when)).trim(), table);
            return description.members().stream().filter(m -> m.assignment().topicPartitions().isEmpty()).count();
        }
    }

    private static void await(LongSupplier value, long target, Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (value.getAsLong() < target) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(what + ": reached " + value.getAsLong() + " of " + target + " within " + timeout);
            }
            DemoSupport.sleep(50);
        }
    }
}
