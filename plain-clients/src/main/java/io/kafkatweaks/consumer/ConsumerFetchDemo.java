package io.kafkatweaks.consumer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Seed;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Chapter 07: the poll loop and fetch tuning.
 * <ol>
 *   <li>catch-up throughput under several fetch presets (records per poll, bytes per fetch, fetch requests)</li>
 *   <li>a slow handler that overruns max.poll.interval.ms, gets kicked out, and the two ways to fix it</li>
 *   <li>low traffic: fetch.min.bytes + fetch.max.wait.ms trade latency for fewer, fuller fetches</li>
 * </ol>
 * <pre>
 *   records=150000  records the topic is (re)seeded with
 *   size=512
 *   runs=a,b        subset of the presets
 * </pre>
 */
public final class ConsumerFetchDemo implements Demo {

    private static final String TOPIC = "tweaks.fetch";

    static final Map<String, Map<String, String>> PRESETS = new LinkedHashMap<>();

    static {
        PRESETS.put("defaults", Map.of());
        PRESETS.put("max.poll.records=50", Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50"));
        PRESETS.put("max.poll.records=5000", Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000"));
        PRESETS.put("max.partition.fetch.bytes=64K", Map.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(64 * 1024)));
        // No spaces or commas in a preset name: `runs=` splits on commas and -Dexec.args splits on whitespace,
        // so a name containing either could never be selected.
        PRESETS.put("fetch.min.bytes=1M+wait=500", Map.of(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(1024 * 1024),
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500"));
        PRESETS.put("big-everything", Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000",
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(4 * 1024 * 1024),
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(1024 * 1024), ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500"));
    }

    @Override
    public void run(Args args) throws Exception {
        long records = args.getLong("records", 150_000);   // a few seconds of catch-up per preset; shorter runs are mostly noise
        int size = args.getInt("size", 512);
        List<String> runs = args.get("runs").map(s -> List.of(s.split(","))).orElse(List.copyOf(PRESETS.keySet()));

        long total;
        try (var topics = new Topics()) {
            // Recreated and seeded with producer DEFAULTS (16 KB batches, no compression): the batches on disk are
            // what fetches return (the broker never re-batches), so the presets below are measured against
            // realistic application batches rather than the 256 KB zstd batches the fast seeder would write.
            topics.recreate(TOPIC, 3);
            total = Seed.ensure(topics, TOPIC, 3, records, size, Map.of());
        }
        Knobs.printConsumer(Env.consumer("knobs", "knobs"),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                ConsumerConfig.RECEIVE_BUFFER_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);

        System.out.println("\n1. catching up on %d records under each preset (fresh group each time, auto.offset.reset=earliest)%n".formatted(total));
        catchUp(args, "warm-up", Map.of(), total);   // JIT warm-up, discarded: the first measured preset must not pay for it
        var table = new Table("preset", "elapsed ms", "records/s", "MB/s", "polls", "records/poll", "fetch-size-avg", "records/fetch", "fetch-latency-avg", "fetch-rate");
        for (String name : runs) {
            Map<String, String> preset = PRESETS.get(name);
            if (preset == null) {
                System.err.println("unknown preset '" + name + "', known: " + PRESETS.keySet());
                continue;
            }
            table.row(catchUp(args, name, preset, total));
        }
        table.print("catch-up throughput");
        System.out.println("""
                  max.poll.records only shapes how many records ONE poll() hands back; the fetch size is decided by the
                  *.fetch.bytes settings and the fetcher runs ahead of poll(). Small polls cost little as long as the
                  fetcher keeps up; tiny fetches (64K) cost a request per 64 KB and show up as fetch-rate and latency.
                """);

        slowHandler(args);
        lowTraffic(args);
    }

    // ------------------------------------------------------------------ 1. catch-up

    private static Object[] catchUp(Args args, String label, Map<String, String> overrides, long total) {
        Properties props = Env.consumer("fetch-" + label.replaceAll("[^a-zA-Z0-9]", "") + "-" + System.nanoTime(), "fetch-" + label);
        props.putAll(overrides);
        args.applyOverrides(props);
        long consumed = 0, bytes = 0, polls = 0;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            // first poll joins the group and gets an assignment; not part of the measurement
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }
            consumer.seekToBeginning(consumer.assignment());
            var watch = Stopwatch.start();
            while (consumed < total) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(2));
                polls++;
                for (var r : batch) {
                    consumed++;
                    bytes += r.serializedValueSize();
                }
                if (batch.isEmpty() && watch.elapsedSeconds() > 60) {
                    break;
                }
            }
            double ms = watch.elapsedMillis();
            var m = consumer.metrics();
            return new Object[] {label, ms, consumed / (ms / 1000d), bytes / 1_048_576d / (ms / 1000d), polls, (double) consumed / polls,
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "fetch-size-avg"),
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "records-per-request-avg"),
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "fetch-latency-avg"),
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "fetch-rate")};
        }
    }

    // ------------------------------------------------------------------ 2. slow handler

    private static void slowHandler(Args args) {
        System.out.println("""

                2. the slow handler. poll() must be called again within max.poll.interval.ms (default 300000) or the
                   consumer is considered dead: it leaves the group, its partitions are reassigned, and its next commit
                   fails. Here the budget is squeezed to 3000 ms and every record "takes" 10 ms.
                """);
        var table = new Table("max.poll.records", "max.poll.interval.ms", "ms per record", "outcome");
        table.row(slowRun(args, 500, 3000, 10));
        table.row(slowRun(args, 100, 3000, 10));
        table.print("slow handler runs (each processes 3 polls then stops)");
        System.out.println("""
                  fix 1: lower max.poll.records so that records x processing time fits in max.poll.interval.ms
                  fix 2: raise max.poll.interval.ms (the honest budget for your slowest batch)
                  fix 3: hand the work to other threads and keep polling: chapter 10
                  (with the classic protocol the same failure surfaces as a rebalance; session.timeout.ms is about the
                   heartbeat thread being alive, max.poll.interval.ms is about YOUR thread making progress)
                """);
    }

    private static Object[] slowRun(Args args, int maxPollRecords, int maxPollIntervalMs, int msPerRecord) {
        Properties props = Env.consumer("fetch-slow-" + System.nanoTime(), "fetch-slow-" + maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        args.applyOverrides(props);
        String outcome;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            int nonEmptyPolls = 0;
            int processed = 0;
            outcome = "ok";
            var watch = Stopwatch.start();
            while (nonEmptyPolls < 3) {
                var batch = consumer.poll(Duration.ofSeconds(2));
                if (batch.isEmpty()) {
                    // Same escape as catchUp: a topic holding fewer records than 3 polls can return would
                    // otherwise keep this loop polling an empty topic forever.
                    if (watch.elapsedSeconds() > 60) {
                        outcome = "topic drained after %d non-empty poll(s), %d records".formatted(nonEmptyPolls, processed);
                        break;
                    }
                    continue;
                }
                nonEmptyPolls++;
                Topics.sleep((long) batch.count() * msPerRecord);   // the "work"
                processed += batch.count();
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    outcome = "CommitFailedException after %d records: %s".formatted(processed, firstSentence(e.getMessage()));
                    break;
                }
            }
            if (outcome.equals("ok")) {
                outcome = "processed %d records in 3 polls, every commit succeeded".formatted(processed);
            }
        }
        return new Object[] {maxPollRecords, maxPollIntervalMs, msPerRecord, outcome};
    }

    // ------------------------------------------------------------------ 3. low traffic

    private static void lowTraffic(Args args) throws Exception {
        System.out.println("""

                3. low traffic: a trickle producer sends ~200 records/s. fetch.min.bytes tells the broker not to answer a fetch
                   until that many bytes are available, fetch.max.wait.ms caps the wait. That turns many tiny fetches into few
                   full ones, at the price of latency.
                """);
        var stop = new AtomicBoolean(false);
        Thread trickle = Thread.ofVirtual().start(() -> {
            var props = Env.producer("fetch-trickle");
            props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
            try (var producer = new KafkaProducer<String, String>(props)) {
                long i = 0;
                while (!stop.get()) {
                    producer.send(new ProducerRecord<>(TOPIC, null, Payloads.json(i++, 256)));
                    Topics.sleep(5);
                }
            }
        });
        try {
            var table = new Table("fetch.min.bytes", "fetch.max.wait.ms", "records", "polls", "non-empty polls", "records/fetch", "fetch-latency-avg ms", "fetch-rate /s");
            table.row(tailRun(args, 1, 500, 8));
            table.row(tailRun(args, 64 * 1024, 2000, 8));
            table.print("tailing the trickle for 8 s");
        } finally {
            stop.set(true);
            trickle.join();
        }
        System.out.println("""
                  fetch.min.bytes=1 (default): the broker answers as soon as there is anything, so each fetch carries a
                  handful of records and the consumer issues many requests.
                  fetch.min.bytes=64K + fetch.max.wait.ms=2000: fetches return every ~2 s with everything that arrived,
                  ~1 request per 2 s. End-to-end latency grew by up to 2 s. Pick the wait you can afford.
                """);
    }

    private static Object[] tailRun(Args args, int fetchMinBytes, int fetchMaxWaitMs, int seconds) {
        Properties props = Env.consumer("fetch-tail-" + System.nanoTime(), "fetch-tail-" + fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(fetchMinBytes));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(fetchMaxWaitMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        args.applyOverrides(props);
        long records = 0, polls = 0, nonEmpty = 0;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }
            consumer.seekToEnd(consumer.assignment());
            long end = System.currentTimeMillis() + seconds * 1000L;
            while (System.currentTimeMillis() < end) {
                var batch = consumer.poll(Duration.ofMillis(100));
                polls++;
                if (!batch.isEmpty()) {
                    nonEmpty++;
                    records += batch.count();
                }
            }
            var m = consumer.metrics();
            return new Object[] {fetchMinBytes, fetchMaxWaitMs, records, polls, nonEmpty,
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "records-per-request-avg"),
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "fetch-latency-avg"),
                    MetricsReport.value(m, MetricsReport.CONSUMER_FETCH, "fetch-rate")};
        }
    }

    private static String firstSentence(String s) {
        int i = s.indexOf(". ");
        return i > 0 ? s.substring(0, i + 1) : s;
    }
}
