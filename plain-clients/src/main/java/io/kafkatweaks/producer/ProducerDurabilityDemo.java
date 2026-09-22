package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Workload;
import io.kafkatweaks.producer.recipe.DurableProducer;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Chapter 03: durability, ordering and retries. Measures {@link DurableProducer}; everything else in this file is
 * measurement.
 * <ol>
 *   <li>acks=0 / 1 / all on the same workload: what each costs and what each promises.</li>
 *   <li>The timeout chain: delivery.timeout.ms must cover linger.ms + request.timeout.ms.</li>
 *   <li>min.insync.replicas in action on an RF=2 topic: stop the one broker that matters and watch
 *       acks=all refuse the write ({@code NotEnoughReplicasException}) while acks=1 "succeeds".</li>
 * </ol>
 * <pre>
 *   records=20000            records per acks run
 *   size=512
 *   broker-control=manual    manual: the demo tells you which container to stop and waits for you
 *                            docker: the demo runs `docker stop/start kafka-N` itself
 *   wait=90                  seconds to wait for the ISR to shrink/recover in manual mode
 *   skip-acks=false          skip part 1
 *   skip-isr=false           skip part 3
 * </pre>
 */
public final class ProducerDurabilityDemo implements Demo {

    private static final String TOPIC_ACKS = "tweaks.durability";
    private static final String TOPIC_RF2 = "tweaks.durability-rf2";

    @Override
    public void run(Args args) throws Exception {
        long records = args.getLong("records", 20_000);
        int size = args.getInt("size", 512);

        try (var topics = new Topics()) {
            topics.ensure(TOPIC_ACKS, 3, DurableProducer.minInSyncReplicas(2));
            topics.ensure(TOPIC_RF2, 1, 2, DurableProducer.minInSyncReplicas(2));   // RF=2: one broker down = too few replicas
        }

        if (!args.getBool("skip-acks", false)) {
            acksComparison(args, records, size);
        }
        timeoutChain();
        if (!args.getBool("skip-isr", false)) {
            minInSyncReplicas(args);
        }
    }

    // ------------------------------------------------------------------ 1. acks

    private static void acksComparison(Args args, long records, int size) {
        Workload.warmUp(TOPIC_ACKS);
        var results = new ArrayList<Workload.Result>();
        // enable.idempotence defaults to true and REQUIRES acks=all; the client refuses acks=0/1 with it on.
        results.add(Workload.run("acks=0", props(args, "acks-0", DurableProducer.fireAndForget()), TOPIC_ACKS, records, size, Workload.Payload.JSON, 0));
        results.add(Workload.run("acks=1", props(args, "acks-1", DurableProducer.leaderOnly()), TOPIC_ACKS, records, size, Workload.Payload.JSON, 0));
        results.add(Workload.run("acks=all, no idempotence", props(args, "acks-all-plain", Map.of(ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")), TOPIC_ACKS, records, size, Workload.Payload.JSON, 0));
        results.add(Workload.run("acks=all + idempotence (default)", props(args, "acks-all-idem", DurableProducer.durable()),
                TOPIC_ACKS, records, size, Workload.Payload.JSON, 0));
        Workload.printComparison(results);

        new Table("acks", "the broker replies when...", "you can lose data when...", "ordering / duplicates")
                .row("0", "never; the client considers the record sent once it left the socket",
                        "always: any network error or leader failure is silent", "no retries, so no duplicates; gaps instead")
                .row("1", "the leader wrote it to its log",
                        "the leader dies before followers replicated the record", "retries can duplicate/reorder without idempotence")
                .row("all", "every in-sync replica wrote it (bounded below by min.insync.replicas)",
                        "only if min.insync.replicas brokers fail at once", "with enable.idempotence: exactly one copy, in order per partition")
                .print("what acks means");

        Knobs.printProducer(props(args, "knobs", Map.of()),
                ProducerConfig.ACKS_CONFIG, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ProducerConfig.RETRIES_CONFIG,
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG, ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG);
        System.out.println("""
                idempotence (default on since 3.0) gives each producer a PID and each record a per-partition sequence
                number, so a retried batch that the broker already has is de-duplicated, and up to 5 in-flight
                requests stay in order. It costs nothing measurable; the only reason to turn it off is acks=0/1.
                """);
    }

    // ------------------------------------------------------------------ 2. timeouts

    private static void timeoutChain() {
        System.out.println("""
                the timeout chain (all producer side):
                  max.block.ms           how long send() may block for metadata / buffer space          default 60000
                  linger.ms              how long a batch waits for company in the accumulator          default 5
                  request.timeout.ms     how long one produce request may wait for the broker            default 30000
                  retry.backoff.ms       pause between retries (exponential up to retry.backoff.max.ms)  default 100 / 1000
                  delivery.timeout.ms    total budget from send() to callback, including all retries     default 120000
                rule enforced by the client: delivery.timeout.ms >= linger.ms + request.timeout.ms
                """);
        var props = Env.producer("timeouts");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
        try (var ignored = new KafkaProducer<String, String>(props)) {
            System.out.println("unexpected: producer accepted an impossible timeout chain");
        } catch (KafkaException e) {
            // The constructor wraps the ConfigException in KafkaException("Failed to construct kafka producer").
            Throwable cause = e.getCause() instanceof ConfigException c ? c : e;
            System.out.println("delivery.timeout.ms=5000 with request.timeout.ms=30000 is rejected at construction:\n  "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage() + "\n");
        }
    }

    // ------------------------------------------------------------------ 3. min.insync.replicas

    private static void minInSyncReplicas(Args args) throws Exception {
        String control = args.get("broker-control", "manual");
        int waitSeconds = args.getInt("wait", 90);

        try (var topics = new Topics()) {
            TopicDescription d = topics.describeExisting(TOPIC_RF2);
            topics.printPartitions(TOPIC_RF2);
            var partition = d.partitions().getFirst();
            // Stop a follower, not the leader: the outcome is the same (ISR drops to 1 < min.insync.replicas)
            // and the reader does not also have to reason about leader election.
            int victim = partition.replicas().stream().map(n -> n.id())
                    .filter(id -> partition.leader() == null || id != partition.leader().id())
                    .findFirst().orElseThrow();
            String container = "kafka-" + victim;

            System.out.printf("""

                    %s has RF=2 and min.insync.replicas=2: BOTH replicas must be in sync for acks=all to succeed.
                    With one of them gone the topic is read-only for acks=all producers, while acks=1 keeps writing to
                    the leader alone. (The cluster itself stays up: the KRaft quorum only needs 2 of 3 controllers.)

                    """, TOPIC_RF2);

            sendOne("before", TOPIC_RF2, "all");

            if (control.equals("docker")) {
                docker("stop", container);
            } else {
                System.out.printf(">>> in another terminal run:   docker compose stop %s%n", container);
            }
            waitForIsr(topics, TOPIC_RF2, 1, waitSeconds);
            topics.printPartitions(TOPIC_RF2);

            sendOne("broker down", TOPIC_RF2, "all");
            sendOne("broker down", TOPIC_RF2, "1");

            if (control.equals("docker")) {
                docker("start", container);
            } else {
                System.out.printf("%n>>> now bring it back:   docker compose start %s%n", container);
            }
            waitForIsr(topics, TOPIC_RF2, 2, waitSeconds);
            topics.printPartitions(TOPIC_RF2);
            sendOne("recovered", TOPIC_RF2, "all");
        }
        System.out.println("""

                takeaway: acks=all is only as strong as min.insync.replicas. RF=3 + min.insync.replicas=2 (the
                cluster default here) survives one broker; RF=2 + min.insync.replicas=2 survives none, and
                RF=3 + min.insync.replicas=1 can acknowledge a record that lives on a single disk.
                """);
    }

    /** One send with a short delivery budget so a refused write shows up in seconds rather than minutes. */
    private static void sendOne(String phase, String topic, String acks) {
        var props = Env.producer("isr-acks-" + acks);
        // The recipe under test: durable() vs leaderOnly(), both with a short delivery budget.
        props.putAll(acks.equals("all") ? DurableProducer.durable() : DurableProducer.leaderOnly());
        props.putAll(DurableProducer.failFast(Duration.ofSeconds(3), Duration.ofSeconds(8), Duration.ofSeconds(10)));
        long t0 = System.nanoTime();
        try (var producer = new KafkaProducer<String, String>(props)) {
            RecordMetadata md = producer.send(new ProducerRecord<>(topic, "k", "phase=" + phase)).get();
            System.out.printf("[%-11s] acks=%-3s -> OK   offset %d after %.0f ms%n", phase, acks, md.offset(), ms(t0));
            if (acks.equals("1") && phase.startsWith("broker")) {
                System.out.println("              ^ the leader accepted it alone; if that broker dies now the record is gone");
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            System.out.printf("[%-11s] acks=%-3s -> FAIL after %.0f ms: %s: %s%n", phase, acks, ms(t0),
                    cause.getClass().getSimpleName(), firstLine(cause.getMessage()));
            System.out.println("              (NotEnoughReplicasException is retriable: the producer kept retrying until delivery.timeout.ms)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitForIsr(Topics topics, String topic, int expectedIsr, int waitSeconds) {
        System.out.printf("waiting up to %ds for ISR of %s-0 to reach %d ...", waitSeconds, topic, expectedIsr);
        long deadline = System.currentTimeMillis() + waitSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            int isr = topics.describe(topic).map(d -> d.partitions().getFirst().isr().size()).orElse(-1);
            if (isr == expectedIsr) {
                System.out.println(" done");
                return;
            }
            Topics.sleep(1000);
            System.out.print('.');
        }
        System.out.println(" timed out (continuing anyway; results below may not show the effect)");
    }

    private static void docker(String action, String container) throws IOException, InterruptedException {
        System.out.printf("%n$ docker %s %s%n", action, container);
        var p = new ProcessBuilder("docker", action, container).inheritIO().start();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("docker " + action + " " + container + " failed");
        }
    }

    private static Properties props(Args args, String clientId, Map<String, ?> overrides) {
        var p = Env.producer("durability-" + clientId);
        p.putAll(overrides);
        return args.applyOverrides(p);
    }

    private static double ms(long t0) {
        return (System.nanoTime() - t0) / 1_000_000d;
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
