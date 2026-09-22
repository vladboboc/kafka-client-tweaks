package io.kafkatweaks.consumer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.common.Seed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 10: getting more processing out of a topic when the handler, not Kafka, is the bottleneck.
 * Every record "costs" {@code work-ms} of processing. Same topic, same records, five ways of consuming:
 * <ol>
 *   <li>one consumer, sequential</li>
 *   <li>one consumer per partition (a group of 6), sequential each</li>
 *   <li>one consumer, the records of each poll processed in parallel per partition (ordering kept)</li>
 *   <li>one consumer, every record in parallel on virtual threads (ordering gone)</li>
 *   <li>one consumer, asynchronous per-partition pipeline with pause/resume back-pressure and watermark commits</li>
 * </ol>
 * <pre>
 *   records=6000   records to process per mode (topic is seeded once)
 *   work-ms=5      simulated processing time per record
 *   modes=1,2,5    subset
 * </pre>
 */
public final class ConsumerParallelDemo implements Demo {

    private static final String TOPIC = "tweaks.parallel";
    private static final int PARTITIONS = 6;

    @Override
    public void run(Args args) throws Exception {
        int records = args.getInt("records", 6000);
        int workMs = args.getInt("work-ms", 5);
        List<String> modes = args.get("modes").map(s -> List.of(s.split(","))).orElse(List.of("1", "2", "3", "4", "5"));

        try (var topics = new Topics()) {
            topics.recreate(TOPIC, PARTITIONS);
            Seed.ensure(topics, TOPIC, PARTITIONS, records, 200);
        }
        System.out.printf("%d records, %d ms of work each = %.1f s of pure processing. Partitions: %d.%n%n",
                records, workMs, records * workMs / 1000d, PARTITIONS);

        var table = new Table("mode", "consumers", "workers", "elapsed s", "records/s", "ordering", "commit safety");
        if (modes.contains("1")) {
            table.row(sequential(args, records, workMs));
        }
        if (modes.contains("2")) {
            table.row(consumerPerPartition(args, records, workMs));
        }
        if (modes.contains("3")) {
            table.row(parallelPerPartitionBatch(args, records, workMs));
        }
        if (modes.contains("4")) {
            table.row(parallelPerRecord(args, records, workMs));
        }
        if (modes.contains("5")) {
            table.row(asyncPipeline(args, records, workMs));
        }
        table.print("results");
        System.out.println("""

                  the unit of parallelism in a consumer GROUP is the partition: more consumers than partitions sit idle.
                  inside one consumer you may go further, as long as you respect two rules:
                    1. call poll() regularly (max.poll.interval.ms) -> hand work to other threads, do not do it inline
                    2. commit only offsets whose records are DONE, per partition, in order -> the watermark in mode 5
                  per-partition workers keep Kafka's ordering guarantee; per-record fan-out (mode 4) throws it away,
                  which is fine for idempotent, independent records and wrong for anything keyed.
                  virtual threads (Java 21+) make "one worker per partition" or "one task per record" cost nothing to create;
                  the bottleneck moves to whatever the handler talks to.
                """);
    }

    // ------------------------------------------------------------------ 1

    private static Object[] sequential(Args args, int records, int workMs) {
        var watch = Stopwatch.start();
        try (var consumer = consumer(args, "parallel-1-" + System.nanoTime(), "mode1")) {
            consumer.subscribe(List.of(TOPIC));
            int done = 0;
            while (done < records) {
                for (var r : consumer.poll(Duration.ofMillis(500))) {
                    work(r, workMs);
                    done++;
                }
                consumer.commitSync();
            }
        }
        return new Object[] {"1 sequential", 1, 1, watch.elapsedSeconds(), watch.rate(records), "per partition", "commit after batch: at-least-once"};
    }

    // ------------------------------------------------------------------ 2

    private static Object[] consumerPerPartition(Args args, int records, int workMs) throws Exception {
        var watch = Stopwatch.start();
        String group = "parallel-2-" + System.nanoTime();
        var done = new AtomicInteger();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < PARTITIONS; i++) {
                final int id = i;
                pool.submit(() -> {
                    try (var consumer = consumer(args, group, "mode2-" + id)) {
                        consumer.subscribe(List.of(TOPIC));
                        while (done.get() < records) {
                            for (var r : consumer.poll(Duration.ofMillis(300))) {
                                work(r, workMs);
                                done.incrementAndGet();
                            }
                            consumer.commitSync();
                        }
                    }
                });
            }
        }
        return new Object[] {"2 consumer per partition", PARTITIONS, PARTITIONS, watch.elapsedSeconds(), watch.rate(records), "per partition", "same as 1, per consumer"};
    }

    // ------------------------------------------------------------------ 3

    private static Object[] parallelPerPartitionBatch(Args args, int records, int workMs) throws Exception {
        var watch = Stopwatch.start();
        try (var consumer = interleavingConsumer(args, "parallel-3-" + System.nanoTime(), "mode3");
             ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            consumer.subscribe(List.of(TOPIC));
            int done = 0;
            while (done < records) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                var futures = new ArrayList<Future<?>>();
                for (TopicPartition tp : batch.partitions()) {
                    List<ConsumerRecord<String, String>> part = batch.records(tp);   // in offset order
                    futures.add(pool.submit(() -> part.forEach(r -> work(r, workMs))));
                }
                for (var f : futures) {
                    f.get();   // the poll thread waits: fine while (records per partition per poll × work) < max.poll.interval.ms
                }
                done += batch.count();
                consumer.commitSync();
            }
        }
        return new Object[] {"3 per-partition tasks per poll", 1, PARTITIONS, watch.elapsedSeconds(), watch.rate(records), "per partition", "commit after whole batch done"};
    }

    // ------------------------------------------------------------------ 4

    private static Object[] parallelPerRecord(Args args, int records, int workMs) throws Exception {
        var watch = Stopwatch.start();
        try (var consumer = consumer(args, "parallel-4-" + System.nanoTime(), "mode4");
             ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            consumer.subscribe(List.of(TOPIC));
            int done = 0;
            while (done < records) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                var futures = new ArrayList<Future<?>>();
                for (var r : batch) {
                    futures.add(pool.submit(() -> work(r, workMs)));
                }
                for (var f : futures) {
                    f.get();
                }
                done += batch.count();
                consumer.commitSync();
            }
        }
        return new Object[] {"4 one task per record", 1, "unbounded (virtual)", watch.elapsedSeconds(), watch.rate(records), "NONE", "commit after whole batch done"};
    }

    // ------------------------------------------------------------------ 5

    /**
     * The production-grade shape: the poll thread never does work and never blocks on it. Each partition
     * has one sequential worker (ordering kept), records flow through a bounded amount of in-flight work
     * (pause when too much, resume when drained), and offsets are committed from a per-partition
     * "everything below this is done" watermark.
     */
    private static Object[] asyncPipeline(Args args, int records, int workMs) throws Exception {
        // Enough buffered work that every partition worker stays busy between polls; pause well before memory matters.
        final int highWatermark = 3000, lowWatermark = 1500;
        var watch = Stopwatch.start();
        var inFlight = new AtomicInteger();
        var completed = new ConcurrentHashMap<TopicPartition, AtomicLong>();   // next offset to commit, per partition
        var workers = new HashMap<TopicPartition, ExecutorService>();
        int pauses = 0;
        long processed = 0;
        boolean paused = false;
        try (var consumer = interleavingConsumer(args, "parallel-5-" + System.nanoTime(), "mode5")) {
            consumer.subscribe(List.of(TOPIC));
            while (processed < records) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(100));
                for (var r : batch) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    // one single-threaded (virtual) executor per partition = FIFO per partition
                    ExecutorService worker = workers.computeIfAbsent(tp, k -> Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()));
                    inFlight.incrementAndGet();
                    worker.submit(() -> {
                        work(r, workMs);
                        completed.computeIfAbsent(tp, k -> new AtomicLong()).set(r.offset() + 1);   // sequential per partition, so this is monotonic
                        inFlight.decrementAndGet();
                    });
                }
                processed = completed.values().stream().mapToLong(AtomicLong::get).sum();
                // back-pressure: stop fetching (but keep polling, so the group still sees us alive) while workers are behind
                if (!paused && inFlight.get() > highWatermark) {
                    consumer.pause(consumer.assignment());
                    paused = true;
                    pauses++;
                } else if (paused && inFlight.get() < lowWatermark) {
                    consumer.resume(consumer.assignment());
                    paused = false;
                }
                // commit the watermarks: only offsets whose records are done
                var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                completed.forEach((tp, next) -> offsets.put(tp, new OffsetAndMetadata(next.get())));
                if (!offsets.isEmpty()) {
                    consumer.commitAsync(offsets, null);
                }
            }
            // The final commit is the watermark map too, never the no-arg commitSync(): that one commits the
            // consumer's POSITION, i.e. everything fetched, which would mark any record still in a worker's
            // queue as processed and break the rule this mode exists to demonstrate.
            var finalOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
            completed.forEach((tp, next) -> finalOffsets.put(tp, new OffsetAndMetadata(next.get())));
            if (!finalOffsets.isEmpty()) {
                consumer.commitSync(finalOffsets);
            }
        } finally {
            workers.values().forEach(ExecutorService::close);
        }
        return new Object[] {"5 async pipeline (paused %dx)".formatted(pauses), 1, PARTITIONS, watch.elapsedSeconds(), watch.rate(records), "per partition",
                "watermark commits: at-least-once, poll never blocks"};
    }

    // ------------------------------------------------------------------ helpers

    private static void work(ConsumerRecord<String, String> record, int ms) {
        if (record.value() == null) {
            return;
        }
        Topics.sleep(ms);
    }

    private static KafkaConsumer<String, String> consumer(Args args, String group, String clientId) {
        Properties props = Env.consumer(group, "parallel-" + clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        args.applyOverrides(props);
        return new KafkaConsumer<>(props);
    }

    /**
     * A poll() hands back records partition by partition: it drains what the fetcher buffered for one
     * partition (up to max.partition.fetch.bytes, 1 MB = thousands of small records) before touching the
     * next. Per-partition fan-out inside one consumer therefore only works when polls SPAN partitions,
     * which means small per-partition fetches. This is the hidden cost of modes 3 and 5, and one reason
     * "one consumer per partition" (mode 2) is the operational default.
     */
    private static KafkaConsumer<String, String> interleavingConsumer(Args args, String group, String clientId) {
        Properties props = Env.consumer(group, "parallel-" + clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(16 * 1024));
        args.applyOverrides(props);
        return new KafkaConsumer<>(props);
    }
}
