package io.kafkatweaks.consumer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Seed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Chapter 09: how partitions move between consumers, under each protocol.
 * The same choreography runs for every scenario: A joins, B joins, C joins, B leaves (and, for static
 * membership, comes back). Every revoke/assign callback is stamped on a timeline so the difference between
 * "stop the world" and incremental rebalancing is visible.
 * <pre>
 *   scenarios=a,b,c   subset of the scenario keys printed by the demo
 *   settle=4          seconds of silence before a state counts as stable
 * </pre>
 */
public final class ConsumerRebalanceDemo implements Demo {

    private static final String TOPIC = "tweaks.rebalance";

    record Scenario(String key, String title, Map<String, String> config, boolean staticMembers) {
    }

    static final List<Scenario> SCENARIOS = List.of(
            new Scenario("consumer-uniform", "group.protocol=consumer (KIP-848), server-side assignor uniform (default)",
                    Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"), false),
            new Scenario("consumer-range", "group.protocol=consumer, group.remote.assignor=range",
                    Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer", ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "range"), false),
            new Scenario("classic-range", "group.protocol=classic (deprecated), RangeAssignor: EAGER, stop-the-world",
                    Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
                            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName()), false),
            new Scenario("classic-cooperative", "group.protocol=classic, CooperativeStickyAssignor: incremental",
                    Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
                            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()), false),
            new Scenario("consumer-static", "group.protocol=consumer + group.instance.id (static membership): B restarts without a rebalance",
                    Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"), true));

    @Override
    public void run(Args args) throws Exception {
        int settleSeconds = args.getInt("settle", 4);
        List<String> keys = args.get("scenarios").map(s -> List.of(s.split(","))).orElse(SCENARIOS.stream().map(Scenario::key).toList());

        try (var topics = new Topics()) {
            Seed.ensure(topics, TOPIC, 6, 6000, 200);
        }
        Knobs.printConsumer(Env.consumer("knobs", "knobs"), ConsumerConfig.GROUP_PROTOCOL_CONFIG,
                ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        System.out.println("""
                  kafka-clients 4.3 still defaults group.protocol to classic; KIP-1274 deprecates classic and logs a warning
                  when it is used. With group.protocol=consumer the assignment is computed by the group coordinator and
                  session.timeout.ms / heartbeat.interval.ms are BROKER settings (group.consumer.session.timeout.ms=45000,
                  group.consumer.heartbeat.interval.ms=5000 in docker-compose.yml); the client-side values are ignored.
                """);

        var summary = new Table("scenario", "rebalances seen by A", "partitions A lost when B joined", "partitions A lost when C joined",
                "A's rebalance-latency-avg ms", "A's rebalance-total");
        for (Scenario s : SCENARIOS) {
            if (!keys.contains(s.key())) {
                continue;
            }
            summary.row(runScenario(args, s, settleSeconds));
        }
        summary.print("summary");
        System.out.println("""
                  eager (classic + Range/RoundRobin): every member gives up ALL partitions on every change, then gets a new set.
                    Processing stops for the whole group for the duration of the rebalance (rebalance-latency).
                  incremental (classic + CooperativeSticky, or the consumer protocol): only the partitions that must move are
                    revoked; the others keep being consumed. The consumer protocol also removes the JoinGroup/SyncGroup
                    round trips: members learn their new assignment through heartbeats.
                  static membership (group.instance.id): a restart within session.timeout.ms is not a leave; the member gets
                    its old partitions back and nobody else notices. Use it for stateful consumers and rolling deploys.
                """);
    }

    private static Object[] runScenario(Args args, Scenario s, int settleSeconds) throws InterruptedException {
        String group = "rebalance-" + s.key() + "-" + System.nanoTime() % 100_000;
        System.out.printf("%n=== %s%n    %s   (group %s)%n", s.key(), s.title(), group);
        var timeline = new Timeline();

        // Every member owns a consumer on its own platform thread, so stopping them is not optional: an exception
        // anywhere below (a settle timeout, a failing admin call) would otherwise leave those threads polling and
        // the JVM alive - Run's System.exit(0) is only reached when a demo returns normally.
        Member a = null, b = null, c = null;
        try {
            a = new Member("A", group, s, args, timeline);
            a.start();
            timeline.waitUntilStable(settleSeconds, a);
            timeline.mark("--- B joins");
            b = new Member("B", group, s, args, timeline);
            b.start();
            timeline.waitUntilStable(settleSeconds, a, b);
            int lostOnB = timeline.revokedByDuring("A", "--- B joins");
            timeline.mark("--- C joins");
            c = new Member("C", group, s, args, timeline);
            c.start();
            timeline.waitUntilStable(settleSeconds, a, b, c);
            int lostOnC = timeline.revokedByDuring("A", "--- C joins");
            timeline.mark("--- B leaves" + (s.staticMembers() ? " (close, static member: no LeaveGroup)" : ""));
            b.stop();
            if (s.staticMembers()) {
                Thread.sleep(2000);
                timeline.mark("--- B restarts with the same group.instance.id");
                b = new Member("B", group, s, args, timeline);
                b.start();
                timeline.waitUntilStable(settleSeconds, a, b, c);
            } else {
                timeline.waitUntilStable(settleSeconds, a, c);
            }
            timeline.print();
            try (var topics = new Topics()) {
                topics.printAssignments(group);
            }
            var m = a.metrics();
            double latency = MetricsReport.value(m, MetricsReport.CONSUMER_COORDINATOR, "rebalance-latency-avg");
            double total = MetricsReport.value(m, MetricsReport.CONSUMER_COORDINATOR, "rebalance-total");
            return new Object[] {s.key(), timeline.assignCount("A"), lostOnB, lostOnC, latency, total};
        } finally {
            stopQuietly(a);
            stopQuietly(b);   // already stopped members return immediately: stop() is idempotent
            stopQuietly(c);
        }
    }

    /** Stops a member without letting an interrupt mask whatever exception is already on its way out. */
    private static void stopQuietly(Member member) {
        if (member == null) {
            return;
        }
        try {
            member.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** One consumer in its own (virtual) thread, reporting every assignment change to the shared timeline. */
    static final class Member {
        final String name;
        final AtomicBoolean running = new AtomicBoolean(true);
        final Timeline timeline;
        final KafkaConsumer<String, String> consumer;
        volatile Set<Integer> assigned = Set.of();
        Thread thread;

        Member(String name, String group, Scenario s, Args args, Timeline timeline) {
            this.name = name;
            this.timeline = timeline;
            Properties props = Env.consumer(group, "member-" + name);
            props.putAll(s.config());
            if (s.staticMembers()) {
                props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-" + name);
            }
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            args.applyOverrides(props);
            this.consumer = new KafkaConsumer<>(props);
        }

        void start() {
            thread = Thread.ofPlatform().name("consumer-" + name).start(() -> {
                try {
                    consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            if (!partitions.isEmpty()) {
                                timeline.add(name, "revoked ", partitions);
                            }
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            timeline.add(name, "assigned", partitions);
                            assigned = snapshot();
                        }

                        @Override
                        public void onPartitionsLost(Collection<TopicPartition> partitions) {
                            timeline.add(name, "LOST    ", partitions);
                        }

                        private Set<Integer> snapshot() {
                            return consumer.assignment().stream().map(TopicPartition::partition).collect(Collectors.toCollection(TreeSet::new));
                        }
                    });
                    while (running.get()) {
                        consumer.poll(Duration.ofMillis(200));
                        Set<Integer> now = consumer.assignment().stream().map(TopicPartition::partition).collect(Collectors.toCollection(TreeSet::new));
                        if (!now.equals(assigned)) {
                            timeline.touch();   // assignment changed without a callback we saw yet: keep the settle timer honest
                        }
                        assigned = now;
                    }
                } catch (WakeupException ignored) {
                    // stop() requested
                } finally {
                    consumer.close();
                }
            });
        }

        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
            return consumer.metrics();
        }

        void stop() throws InterruptedException {
            if (!running.getAndSet(false)) {
                return;
            }
            consumer.wakeup();
            thread.join();
            assigned = Set.of();
        }
    }

    /** Timestamped list of rebalance callbacks, shared by all members of one scenario. */
    static final class Timeline {
        private final long start = System.nanoTime();
        private final List<String> lines = new CopyOnWriteArrayList<>();
        private volatile long lastEventNanos = System.nanoTime();

        void touch() {
            lastEventNanos = System.nanoTime();
        }

        void add(String member, String what, Collection<TopicPartition> partitions) {
            String ps = partitions.stream().map(TopicPartition::partition).sorted().map(String::valueOf).collect(Collectors.joining(","));
            lines.add("%7.2fs  %s %s [%s]".formatted(seconds(), member, what, ps));
            lastEventNanos = System.nanoTime();
        }

        void mark(String text) {
            lines.add("%7.2fs  %s".formatted(seconds(), text));
            lastEventNanos = System.nanoTime();   // the settle timer starts at the event, not at the last callback
        }

        /**
         * Waits until every live member owns at least one partition, the live members together own all 6,
         * and no callback has fired for {@code settle} seconds.
         */
        void waitUntilStable(int settle, Member... live) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 90_000;
            while (System.currentTimeMillis() < deadline) {
                int owned = 0;
                boolean everyoneHasSome = true;
                for (Member m : live) {
                    owned += m.assigned.size();
                    everyoneHasSome &= !m.assigned.isEmpty();
                }
                boolean quiet = (System.nanoTime() - lastEventNanos) > settle * 1_000_000_000L;
                if (owned == 6 && everyoneHasSome && quiet) {
                    return;
                }
                Thread.sleep(100);
            }
            mark("(timed out waiting for a stable assignment)");
        }

        int revokedByDuring(String member, String afterMark) {
            int idx = -1;
            for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).endsWith(afterMark)) {
                    idx = i;
                }
            }
            int lost = 0;
            for (int i = idx + 1; i < lines.size(); i++) {
                String l = lines.get(i);
                if (l.contains("---") && i > idx + 1 && !l.endsWith(afterMark)) {
                    break;
                }
                if (l.contains(" " + member + " revoked ")) {
                    lost += l.substring(l.indexOf('[') + 1, l.indexOf(']')).split(",").length;
                }
            }
            return lost;
        }

        int assignCount(String member) {
            return (int) lines.stream().filter(l -> l.contains(" " + member + " assigned")).count();
        }

        void print() {
            System.out.println("timeline:");
            new ArrayList<>(lines).forEach(l -> System.out.println("  " + l));
        }

        private double seconds() {
            return (System.nanoTime() - start) / 1e9;
        }
    }
}
