package io.kafkatweaks.consumer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.consumer.recipe.ShareWorker;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Chapter 11: Queues for Kafka (KIP-932, production-ready since Kafka 4.2). A share group hands records of
 * the same partition to many consumers at once, tracks per-record acknowledgement and delivery counts,
 * and re-delivers what was released or left unacknowledged. Partitions stop being the unit of parallelism.
 * Measures {@link ShareWorker}; everything else in this file is measurement.
 * <ol>
 *   <li>4 share consumers on 3 partitions, implicit acknowledgement</li>
 *   <li>explicit acknowledgement: ACCEPT / RELEASE (retry) / REJECT (dead)</li>
 *   <li>acquisition locks: a consumer that goes silent loses its records to the others</li>
 * </ol>
 * <pre>
 *   records=3000
 *   consumers=4
 * </pre>
 */
public final class ConsumerShareDemo implements Demo {

    private static final String TOPIC = "tweaks.queue";
    /** One partition, so that both consumers of the lock demo are necessarily assigned the same partition. */
    private static final String LOCK_TOPIC = "tweaks.queue-locks";

    @Override
    public void run(Args args) throws Exception {
        int records = args.getInt("records", 3000);
        int consumers = args.getInt("consumers", 4);
        String group = "queue-" + System.nanoTime() % 1_000_000;

        try (var topics = new Topics()) {
            topics.recreate(TOPIC, 3);
            topics.recreate(LOCK_TOPIC, 1);
            configureGroup(topics, group);
        }
        var knobs = shareProps(args, group, "knobs");
        Knobs.printConsumer(knobs, ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        System.out.println("""
                  group-level configs (set with kafka-configs --entity-type groups, or Admin.incrementalAlterConfigs as here):
                    share.auto.offset.reset          where a NEW share group starts: latest (default) or earliest
                    share.record.lock.duration.ms    how long a delivered record stays locked to a consumer (default 30000)
                    share.delivery.count.limit       deliveries before a record is archived (default 5)
                    share.isolation.level            read_uncommitted (default) / read_committed
                """);

        implicitAcks(args, group, records, consumers);
        explicitAcks(args, group + "-explicit", records);
        lockTimeout(args, group + "-locks");
        System.out.println("""

                  consumer group: partition = unit of parallelism, offsets = the only state, in-order per partition,
                                  one member per partition, redelivery only by seeking.
                  share group:    record = unit of work, per-record ack + delivery count, any member gets any record,
                                  no ordering across members, built-in retry (RELEASE) and dead-lettering (REJECT).
                  use a share group when the work is independent per record and slow/uneven; keep a consumer group when
                  order per key matters or when you need replay by offset.
                """);
    }

    private static void configureGroup(Topics topics, String group) {
        for (String g : List.of(group, group + "-explicit", group + "-locks")) {
            // The lock demo's group starts at LATEST (the default) so that only the records seeded after it
            // subscribed exist for it; the other two need EARLIEST to see the pre-seeded topic.
            String reset = g.endsWith("-locks") ? "latest" : "earliest";
            try {
                ShareWorker.configureGroup(topics.admin(), g, ShareWorker.groupSettings(reset, Duration.ofSeconds(2), 3));
            } catch (ExecutionException e) {
                System.out.println("could not set group configs for " + g + ": " + e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        System.out.printf("group configs set for %s*: share.auto.offset.reset=earliest (latest for the lock demo), share.record.lock.duration.ms=2000, share.delivery.count.limit=3%n", group);
    }

    // ------------------------------------------------------------------ 1. implicit

    private static void implicitAcks(Args args, String group, int records, int consumers) throws Exception {
        System.out.printf("%n1. %d share consumers on %d partitions, share.acknowledgement.mode=implicit (records are accepted%n"
                + "   when the next poll() or commitSync() happens). All consumers get work; %d records total.%n%n", consumers, 3, records);
        seed(TOPIC, records);
        var total = new AtomicInteger();
        var perConsumer = new ConcurrentHashMap<String, AtomicInteger>();
        var perConsumerPartitions = new ConcurrentHashMap<String, TreeSet<Integer>>();
        var seen = new ConcurrentHashMap<String, AtomicInteger>();
        long deadline = System.currentTimeMillis() + 90_000;
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < consumers; i++) {
                String name = "share-" + i;
                pool.submit(() -> {
                    try (var consumer = new KafkaShareConsumer<String, String>(shareProps(args, group, name))) {
                        consumer.subscribe(List.of(TOPIC));
                        // Members get their assignment through heartbeats (group.share.heartbeat.interval.ms, 5 s),
                        // so the first records can take a few seconds to arrive; poll until the group has drained the topic.
                        while (total.get() < records && System.currentTimeMillis() < deadline) {
                            ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(300));
                            for (ConsumerRecord<String, String> r : batch) {
                                perConsumer.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
                                perConsumerPartitions.computeIfAbsent(name, k -> new TreeSet<>()).add(r.partition());
                                seen.computeIfAbsent(r.partition() + "-" + r.offset(), k -> new AtomicInteger()).incrementAndGet();
                                total.incrementAndGet();
                            }
                        }
                        consumer.commitSync();   // acknowledge the last poll's records before leaving
                    }
                });
            }
            // Describe the group once records are flowing (members have their assignment by then).
            while (total.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            try (var topics = new Topics()) {
                describe(topics, group);
            }
        }
        var table = new Table("consumer", "records received", "from partitions");
        new TreeMap<>(perConsumer).forEach((c, n) -> table.row(c, n.get(),
                perConsumerPartitions.get(c).stream().map(String::valueOf).collect(Collectors.joining(","))));
        long dupes = seen.values().stream().filter(c -> c.get() > 1).count();
        table.row("total", total.get(), "distinct records %d, delivered more than once %d".formatted(seen.size(), dupes));
        table.print("distribution");
        System.out.println("""
                  a 4th consumer in a consumer GROUP on 3 partitions would have received nothing.
                  note the assignment table: the share assignor spreads members over partitions so that every partition
                  has at least one member and members share partitions; a member does NOT necessarily see every partition.
                """);
    }

    // ------------------------------------------------------------------ 2. explicit

    private static void explicitAcks(Args args, String group, int records) {
        System.out.println("""

                2. share.acknowledgement.mode=explicit: the application decides per record.
                     ACCEPT  done.                RELEASE  give it back, someone (maybe me) will get it again (delivery count +1).
                     REJECT  never again (poison). Records released too often hit share.delivery.count.limit and are archived.
                   Here: every 50th key is poison -> REJECT; every 7th key fails on its FIRST delivery -> RELEASE, then ACCEPT.
                """);
        var props = shareProps(args, group, "explicit");
        props.putAll(ShareWorker.explicitAcks());
        var accepted = new AtomicInteger();
        var released = new AtomicInteger();
        var rejected = new AtomicInteger();
        var redelivered = new AtomicInteger();
        var maxDelivery = new AtomicInteger();
        try (var consumer = new KafkaShareConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            // The recipe under test; the decider is the demo's scripted policy plus the counters for the table.
            var worker = new ShareWorker<>(consumer, r -> {
                int delivery = r.deliveryCount().map(Short::intValue).orElse(1);
                maxDelivery.accumulateAndGet(delivery, Math::max);
                if (delivery > 1) {
                    redelivered.incrementAndGet();
                }
                long seq = Long.parseLong(r.key().substring(r.key().lastIndexOf('-') + 1));
                if (seq % 50 == 0) {
                    rejected.incrementAndGet();
                    return AcknowledgeType.REJECT;
                }
                if (seq % 7 == 0 && delivery == 1) {
                    released.incrementAndGet();
                    return AcknowledgeType.RELEASE;
                }
                accepted.incrementAndGet();
                return AcknowledgeType.ACCEPT;
            }, (tp, e) -> System.out.println("  commit error on " + tp + ": " + e));
            int idle = 0;
            while (accepted.get() + rejected.get() < records && idle < 20) {
                idle = worker.pollOnce(Duration.ofMillis(300)) == 0 ? idle + 1 : 0;
            }
        }
        new Table("input records", "accepted", "released (retried)", "rejected (poison)", "records seen with deliveryCount > 1", "max deliveryCount")
                .row(records, accepted.get(), released.get(), rejected.get(), redelivered.get(), maxDelivery.get())
                .print("explicit acknowledgement");
        System.out.println("  accepted + rejected = input: every record ended in exactly one final state; released ones came back and were accepted.");
    }

    // ------------------------------------------------------------------ 3. locks

    private static void lockTimeout(Args args, String group) throws Exception {
        System.out.println("""

                3. acquisition locks. A delivered record is locked to its consumer for share.record.lock.duration.ms (2 s here).
                   Consumer A polls, then goes silent (no ack, no poll). Consumer B polls: after the lock expires it receives A's
                   records with deliveryCount=2. A's late acknowledgement is then refused.
                   (on a 1-partition topic, so that A and B are guaranteed to share the partition)
                """);
        var propsA = shareProps(args, group, "A");
        propsA.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        propsA.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        var propsB = shareProps(args, group, "B");
        propsB.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");

        try (var a = new KafkaShareConsumer<String, String>(propsA); var b = new KafkaShareConsumer<String, String>(propsB)) {
            // This group starts at LATEST: A joins first, then 60 records are produced, so those are the only
            // records this group will ever see, and whatever A does not acknowledge is all B can receive.
            a.subscribe(List.of(LOCK_TOPIC));
            long joinDeadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < joinDeadline && !isMember(group, "queue-A")) {
                a.poll(Duration.ofMillis(300));
            }
            seed(LOCK_TOPIC, 60);
            var held = new java.util.ArrayList<ConsumerRecord<String, String>>();
            long acquireDeadline = System.currentTimeMillis() + 30_000;
            while (held.isEmpty() && System.currentTimeMillis() < acquireDeadline) {
                a.poll(Duration.ofMillis(300)).forEach(held::add);
            }
            var heldIds = new TreeSet<String>();
            held.forEach(r -> heldIds.add(r.partition() + "-" + r.offset()));
            System.out.printf("A acquired %d records and stops responding for 3 s...%n", held.size());
            Thread.sleep(3000);

            b.subscribe(List.of(LOCK_TOPIC));
            int reacquired = 0, firstDeliveries = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (reacquired < heldIds.size() && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : b.poll(Duration.ofMillis(300))) {
                    boolean wasHeldByA = heldIds.contains(r.partition() + "-" + r.offset());
                    int delivery = r.deliveryCount().map(Short::intValue).orElse(1);
                    if (wasHeldByA && delivery > 1) {
                        reacquired++;
                    } else {
                        firstDeliveries++;
                    }
                    b.acknowledge(r, AcknowledgeType.ACCEPT);
                }
                b.commitSync();
            }
            System.out.printf("B received %d of A's records again (deliveryCount 2) plus %d fresh ones%n", reacquired, firstDeliveries);

            held.forEach(r -> a.acknowledge(r, AcknowledgeType.ACCEPT));
            Map<TopicIdPartition, Optional<KafkaException>> late = a.commitSync();
            if (late.isEmpty()) {
                System.out.println("A's late acknowledgement: nothing to send, the client had already dropped the expired acquisitions");
            }
            late.forEach((tp, err) -> System.out.printf("A's late acknowledgement for %s: %s%n", tp,
                    err.map(e -> "refused with " + e.getClass().getSimpleName() + " (" + e.getMessage() + ")")
                       .orElse("no error reported; the records' final state had already been set by B, A's ack changed nothing")));
        }
        System.out.println("""
                  the lock is the queue's liveness guarantee: a crashed or stuck consumer cannot hold records hostage.
                  size share.record.lock.duration.ms above your slowest honest processing time, or RENEW the lock
                  (AcknowledgeType.RENEW, KIP-1222) from long-running handlers.
                """);
    }

    // ------------------------------------------------------------------ helpers

    private static void seed(String topic, int records) {
        try (var producer = new KafkaProducer<String, String>(Env.producer("queue-seed"))) {
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>(topic, "job-" + i, Payloads.json(i, 200)));
            }
            producer.flush();
        }
        System.out.printf("seeded %d records into %s%n", records, topic);
    }

    /** Whether the share group currently lists a member with this client id (i.e. the join completed). */
    private static boolean isMember(String group, String clientId) {
        try (var topics = new Topics()) {
            ShareGroupDescription d = topics.admin().describeShareGroups(List.of(group)).all().get().get(group);
            return d.members().stream().anyMatch(m -> m.clientId().equals(clientId) && !m.assignment().topicPartitions().isEmpty());
        } catch (Exception e) {
            return false;
        }
    }

    private static void describe(Topics topics, String group) throws Exception {
        ShareGroupDescription d = topics.admin().describeShareGroups(List.of(group)).all().get().get(group);
        var table = new Table("member (client.id)", "assigned partitions");
        d.members().forEach(m -> table.row(m.clientId(), m.assignment().topicPartitions().stream()
                .map(tp -> String.valueOf(tp.partition())).sorted().collect(Collectors.joining(","))));
        table.print("share group %s: state %s".formatted(group, d.groupState()));
    }

    private static Properties shareProps(Args args, String group, String clientId) {
        var p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "queue-" + clientId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        return args.applyOverrides(p);
    }
}
