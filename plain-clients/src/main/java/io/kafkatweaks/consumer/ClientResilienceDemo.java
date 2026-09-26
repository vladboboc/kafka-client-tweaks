package io.kafkatweaks.consumer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Seed;
import io.kafkatweaks.consumer.recipe.LatencyConsumerInterceptor;
import io.kafkatweaks.consumer.recipe.ResilientClients;
import io.kafkatweaks.consumer.recipe.StampingProducerInterceptor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Chapter 12: the knobs that decide how a client behaves when the cluster misbehaves, plus the hooks
 * operators use to see what clients are doing. Measures {@link ResilientClients} and the two interceptors of the
 * recipe package; everything else in this file is measurement.
 * <ol>
 *   <li>client.rack: follower fetching, and where the bytes actually come from</li>
 *   <li>a broker dies mid-stream: retries, idempotence, backoffs, and how many records were lost (none)</li>
 *   <li>interceptors: cross-cutting hooks on both clients</li>
 *   <li>telemetry: client instance ids (KIP-714) and JMX</li>
 * </ol>
 * <pre>
 *   broker-control=manual|docker    docker: the demo stops/starts kafka-2 itself
 *   rate=1500                       records/s for the failure run
 *   seconds=24                      duration of the failure run (broker stopped at 6 s, restarted at 14 s)
 * </pre>
 */
public final class ClientResilienceDemo implements Demo {

    private static final Logger log = LoggerFactory.getLogger(ClientResilienceDemo.class);
    private static final String TOPIC = "tweaks.resilience";

    @Override
    public void run(Args args) throws Exception {
        try (var topics = new Topics()) {
            topics.recreate(TOPIC, 3);
            Seed.ensure(topics, TOPIC, 3, 6000, 512);
            topics.logPartitions(TOPIC);
        }
        rackAwareFetching(args);
        brokerFailure(args);
        interceptors(args);
        telemetry(args);
        log.info("""
                other knobs in this family (all clients):
                  metadata.recovery.strategy=rebootstrap (default since 4.0): when EVERY known broker is unreachable, the
                      client re-resolves bootstrap.servers instead of spinning on stale metadata. List all brokers, or a
                      load balancer / DNS alias, in bootstrap.servers so this can work.
                  reconnect.backoff.ms / reconnect.backoff.max.ms (50 / 1000): exponential backoff per broker connection;
                      raise them on big fleets so a broker restart does not get a connection storm.
                  connections.max.idle.ms (540000): idle connections are closed; the next request pays a reconnect.
                  socket.connection.setup.timeout.ms / .max.ms (10000 / 30000): how long a TCP connect may take.
                  request.timeout.ms (30000 producer / 30000 consumer) and, for consumers, default.api.timeout.ms (60000)
                      for the blocking calls (commitSync, position, partitionsFor...).""");
    }

    // ------------------------------------------------------------------ 1. client.rack

    private static void rackAwareFetching(Args args) {
        log.info("""
                1. client.rack + follower fetching. Brokers advertise broker.rack (rack-a/b/c here) and run the
                   RackAwareReplicaSelector. A consumer that declares client.rack=rack-b is pointed at the replica living
                   on broker 2 for every partition, whether or not that replica is the leader. Same data, local traffic.""");
        var table = new Table("consumer", "bytes fetched from broker 1", "from broker 2", "from broker 3");
        table.row(fetchBytesPerNode(args, "no client.rack (leader fetching)", null));
        table.row(fetchBytesPerNode(args, "client.rack=rack-b", "rack-b"));
        log.info("consumer-node-metrics / incoming-byte-total per broker connection\n{}", table);
        log.info("with client.rack, all fetched bytes come from the rack-b broker; without it, from each partition's leader.");
    }

    private static Object[] fetchBytesPerNode(Args args, String label, String rack) {
        Properties props = Env.consumer("resilience-rack-" + System.nanoTime(), "resilience-rack");
        if (rack != null) {
            props.putAll(ResilientClients.rackAware(rack));   // <- the recipe under test
        }
        args.applyOverrides(props);
        var perNode = new TreeMap<Integer, Double>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long consumed = 0;
            int idle = 0;
            while (consumed < 6000 && idle < 10) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(300));
                if (batch.isEmpty()) {
                    idle++;   // ten CONSECUTIVE empty polls end the read, so a fetch stall mid-topic does not
                } else {
                    idle = 0;
                    consumed += batch.count();
                }
            }
            for (Map.Entry<MetricName, ? extends Metric> e : consumer.metrics().entrySet()) {
                MetricName mn = e.getKey();
                if (mn.group().equals("consumer-node-metrics") && mn.name().equals("incoming-byte-total")) {
                    String nodeTag = mn.tags().get("node-id");   // "node-1" for broker 1; coordinator connections have huge negative ids
                    int id = Integer.parseInt(nodeTag.substring("node-".length()));
                    if (id >= 0 && id < 100) {
                        perNode.merge(id, ((Number) e.getValue().metricValue()).doubleValue(), Double::sum);
                    }
                }
            }
        }
        return new Object[] {label, perNode.getOrDefault(1, 0d), perNode.getOrDefault(2, 0d), perNode.getOrDefault(3, 0d)};
    }

    // ------------------------------------------------------------------ 2. broker failure

    private static void brokerFailure(Args args) throws Exception {
        String control = args.get("broker-control", "manual");
        int rate = args.getInt("rate", 1500);
        int seconds = args.getInt("seconds", 24);
        log.info("""
                2. a broker dies mid-stream. A producer sends {} records/s for {} s with client defaults (acks=all, idempotence,
                   retries=MAX, delivery.timeout.ms=120000). At 6 s broker kafka-2 stops; at 14 s it comes back. Leaders move,
                   the producer refreshes metadata and retries; nothing is lost, nothing is duplicated.""", rate, seconds);
        Properties props = Env.producer("resilience-failure");
        props.putAll(ResilientClients.outageTolerantProducer());   // <- the recipe under test: the 4.x defaults, written out
        args.applyOverrides(props);
        Knobs.logProducer(props, ProducerConfig.RETRIES_CONFIG, ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG);
        if (!control.equals("docker")) {
            log.info(">>> manual mode: run `docker compose stop kafka-2` a few seconds in, and `docker compose start kafka-2` ~8 s later");
        }

        var acked = new AtomicLong();
        var failed = new AtomicLong();
        long sent = 0;
        var timeline = new Table("t (s)", "sent", "acked", "failed", "record-retry-total", "record-error-total", "event");
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.partitionsFor(TOPIC);
            long start = System.nanoTime();
            long intervalNanos = rate > 0 ? 1_000_000_000L / rate : 0;   // rate=0: as fast as the producer allows
            long next = start;
            long nextTick = start + 1_000_000_000L;
            int second = 0;
            boolean stopped = false, started = false;
            while (System.nanoTime() - start < seconds * 1_000_000_000L) {
                while (System.nanoTime() < next) {
                    LockSupport.parkNanos(50_000);
                }
                next += intervalNanos;
                producer.send(new ProducerRecord<>(TOPIC, "seq-" + sent, Payloads.json(sent, 256)), (RecordMetadata md, Exception ex) -> {
                    if (ex == null) {
                        acked.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                });
                sent++;
                if (System.nanoTime() >= nextTick) {
                    second++;
                    nextTick += 1_000_000_000L;
                    String event = "";
                    if (second == 6 && control.equals("docker") && !stopped) {
                        dockerAsync("stop", "kafka-2");   // off the sending thread: the loop must keep producing
                        stopped = true;
                        event = "docker stop kafka-2";
                    } else if (second == 14 && control.equals("docker") && !started) {
                        dockerAsync("start", "kafka-2");
                        started = true;
                        event = "docker start kafka-2";
                    }
                    var m = producer.metrics();
                    timeline.row(second, sent, acked.get(), failed.get(),
                            MetricsReport.value(m, MetricsReport.PRODUCER, "record-retry-total"),
                            MetricsReport.value(m, MetricsReport.PRODUCER, "record-error-total"), event);
                }
            }
            producer.flush();
            var m = producer.metrics();
            timeline.row("end", sent, acked.get(), failed.get(),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "record-retry-total"),
                    MetricsReport.value(m, MetricsReport.PRODUCER, "record-error-total"), "flush()");
        }
        log.info("timeline\n{}", timeline);

        // Verify: every sequence number exactly once in the topic (from the seed offset on).
        var keys = new HashSet<String>();
        long total = 0, dupes = 0;
        Properties cp = Env.consumer("resilience-verify-" + System.nanoTime(), "resilience-verify");
        try (var consumer = new KafkaConsumer<String, String>(cp)) {
            consumer.subscribe(List.of(TOPIC));
            int idle = 0;
            while (idle < 6) {
                var batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    idle++;
                    continue;
                }
                idle = 0;
                for (var r : batch) {
                    if (r.key() != null && r.key().startsWith("seq-")) {
                        total++;
                        if (!keys.add(r.key())) {
                            dupes++;
                        }
                    }
                }
            }
        }
        // Exact counts, pre-formatted: this table's whole point is that the six numbers line up record for record,
        // which a humanised "36.0K" would hide (it covers everything from 35 950 to 36 049).
        var verification = new Table("sent", "acked", "failed", "in topic", "distinct", "duplicates")
                .row(String.valueOf(sent), String.valueOf(acked.get()), String.valueOf(failed.get()),
                        String.valueOf(total), String.valueOf(keys.size()), String.valueOf(dupes));
        log.info("verification (records keyed seq-*)\n{}", verification);
        log.info("""
                record-retry-total counts the records re-sent while the partitions led by kafka-2 moved to another broker;
                  idempotence made those retries safe (no duplicates), acks=all + min.insync.replicas=2 made them complete.
                  The pause you see in the acked column is leader election + metadata refresh + retry.backoff.ms.""");
    }

    // ------------------------------------------------------------------ 3. interceptors

    private static void interceptors(Args args) {
        log.info("""
                3. interceptors: code that runs inside the client on every send / ack / poll / commit, configured by class
                   name (interceptor.classes). Tracing agents, schema checks, header stamping, audit counters live here.""");
        Properties pp = Env.producer("resilience-interceptor");
        pp.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, StampingProducerInterceptor.class.getName());   // <- the recipe under test
        try (var producer = new KafkaProducer<String, String>(args.applyOverrides(pp))) {
            for (int i = 0; i < 500; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "stamped-" + i, "x"));
            }
        }
        Properties cp = Env.consumer("resilience-interceptor-" + System.nanoTime(), "resilience-interceptor");
        cp.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LatencyConsumerInterceptor.class.getName());   // <- and its counterpart
        try (var consumer = new KafkaConsumer<String, String>(args.applyOverrides(cp))) {
            consumer.subscribe(List.of(TOPIC));
            int idle = 0;
            while (LatencyConsumerInterceptor.RECORDS.get() < 500 && idle < 10) {
                // Consecutive, not cumulative: this group starts at the beginning of a topic that now holds tens of
                // thousands of records, and an empty poll during that scan must not eat into the budget.
                if (consumer.poll(Duration.ofMillis(300)).isEmpty()) {
                    idle++;
                } else {
                    idle = 0;
                }
                consumer.commitSync();
            }
        }
        var counters = new Table("producer onSend", "producer onAcknowledgement", "consumer records with header", "avg produce->consume ms", "consumer onCommit")
                .row(StampingProducerInterceptor.SENT.get(), StampingProducerInterceptor.ACKED.get(), LatencyConsumerInterceptor.RECORDS.get(),
                        LatencyConsumerInterceptor.RECORDS.get() == 0 ? 0 : (double) LatencyConsumerInterceptor.LATENCY_SUM.get() / LatencyConsumerInterceptor.RECORDS.get(),
                        LatencyConsumerInterceptor.COMMITS.get());
        log.info("interceptor counters\n{}", counters);
    }

    // ------------------------------------------------------------------ 4. telemetry

    private static void telemetry(Args args) {
        log.info("""
                4. telemetry. Every client exposes its metrics over JMX (kafka.producer:type=producer-metrics,client-id=...) and,
                   since KIP-714 (enable.metrics.push=true by default), can PUSH them to the brokers, where a metrics plugin
                   collects them centrally (kafka-client-metrics.sh --alter --name ... --metrics ... --interval ...).
                   The broker hands each client a unique instance id, useful to correlate logs, quotas and metrics:""");
        Properties pp = Env.producer("resilience-telemetry");
        try (var producer = new KafkaProducer<String, String>(args.applyOverrides(pp))) {
            producer.partitionsFor(TOPIC);
            try {
                // The handshake (GetTelemetrySubscriptions) runs in the background on the sender thread; the id is
                // null until it has completed, so a fresh client may not have one yet. Long-running clients do.
                var id = ResilientClients.instanceId(producer, Duration.ofSeconds(5));
                log.info("producer client instance id: {}", id.map(String::valueOf).orElse("not negotiated yet (null): ask again later in a long-running client"));
            } catch (Exception e) {
                log.warn("clientInstanceId(): {} {}", e.getClass().getSimpleName(), e.getMessage());
            }
        }
        log.info("""
                JMX: run any demo with -Dcom.sun.management.jmxremote and attach JConsole / a Prometheus JMX exporter; the
                  MBean names are the metric groups this tutorial has been logging, keyed by client-id.""");
    }

    private static void dockerAsync(String action, String container) {
        Thread.ofVirtual().start(() -> {
            try {
                var p = new ProcessBuilder("docker", action, container).redirectErrorStream(true).start();
                p.getInputStream().readAllBytes();
                if (p.waitFor() != 0) {
                    log.warn("docker {} {} failed", action, container);
                }
            } catch (IOException | InterruptedException e) {
                log.warn("docker {} {}: {}", action, container, e.toString());
            }
        });
    }
}
