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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Chapter 08: offsets are the consumer's only durable state. Where you commit them decides what a crash costs.
 * <ol>
 *   <li>the same crash under three commit strategies: at-most-once, at-least-once, at-least-once + idempotent handler</li>
 *   <li>auto-commit: watching committed offset trail the position, and what it implies</li>
 *   <li>auto.offset.reset for a group without offsets: earliest / latest / none</li>
 *   <li>seeking: replay from a timestamp, from the beginning, or N records back</li>
 * </ol>
 * <pre>
 *   records=3000    input records (the topic is recreated)
 *   crash-at=1234   record index at which the consumer "crashes" mid-batch
 * </pre>
 */
public final class ConsumerOffsetsDemo implements Demo {

    private static final String TOPIC = "tweaks.offsets";

    @Override
    public void run(Args args) {
        int records = args.getInt("records", 3000);
        int crashAt = args.getInt("crash-at", 1234);

        try (var topics = new Topics()) {
            topics.recreate(TOPIC, 3);
            Seed.ensure(topics, TOPIC, 3, records, 200);
        }
        Knobs.printConsumer(Env.consumer("knobs", "knobs"), ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.ISOLATION_LEVEL_CONFIG);

        crashStrategies(args, records, crashAt);
        autoCommit(args);
        offsetReset(args, records);
        seeking(args, records);
    }

    // ------------------------------------------------------------------ 1. crash under three strategies

    enum Strategy { COMMIT_BEFORE_PROCESSING, COMMIT_AFTER_PROCESSING, COMMIT_AFTER_IDEMPOTENT }

    private static void crashStrategies(Args args, int records, int crashAt) {
        System.out.printf("%n1. the consumer processes records, and \"crashes\" after record #%d in the middle of a batch (closes without%n"
                + "   committing). A new instance of the same group then finishes the topic. What does the output look like?%n%n", crashAt);
        var table = new Table("strategy", "processed (run 1 + run 2)", "distinct records", "missing", "duplicates (handler saw twice)");
        for (var s : Strategy.values()) {
            String group = "offsets-" + s.name().toLowerCase() + "-" + System.nanoTime();
            var seen = new HashMap<String, Integer>();   // key -> times the handler processed it
            var handled = new HashSet<String>();         // the idempotent handler's durable memory (a DB table in real life): survives the crash
            int run1 = consume(args, group, s, crashAt, seen, handled);
            int run2 = consume(args, group, s, Integer.MAX_VALUE, seen, handled);
            long missing = records - seen.size();
            long dupes = seen.values().stream().filter(c -> c > 1).count();
            table.row(s.name().toLowerCase().replace('_', ' '), run1 + " + " + run2, seen.size(), missing, dupes);
        }
        table.print("%d records, crash after #%d".formatted(records, crashAt));
        System.out.println("""
                  commit before processing  = at-most-once: the records of the interrupted batch are never processed (missing > 0)
                  commit after processing   = at-least-once: the interrupted batch is processed again (duplicates > 0)
                  + idempotent handler      = effectively-once: same redelivery, but the handler recognises what it already did
                                              (dedupe by key/event id, upsert, conditional write, the inbox pattern)
                  exactly-once end to end exists only inside Kafka -> Kafka pipelines (chapter 06's transactions).
                """);
    }

    /** Returns the number of records the handler processed in this run. */
    private static int consume(Args args, String group, Strategy strategy, int crashAfter, Map<String, Integer> seen, Set<String> handled) {
        Properties props = Env.consumer(group, "offsets-" + strategy.name().toLowerCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        args.applyOverrides(props);
        int processed = 0;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            int idle = 0;
            while (idle < 4) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    idle++;
                    continue;
                }
                idle = 0;
                if (strategy == Strategy.COMMIT_BEFORE_PROCESSING) {
                    consumer.commitSync();   // position is already past this batch: commits "we will have processed these"
                }
                for (ConsumerRecord<String, String> r : batch) {
                    String id = r.partition() + "-" + r.offset();
                    if (strategy == Strategy.COMMIT_AFTER_IDEMPOTENT && !handled.add(id)) {
                        continue;   // already done in a previous batch/run: skip
                    }
                    seen.merge(id, 1, Integer::sum);
                    processed++;
                    if (processed >= crashAfter) {
                        return processed;   // crash: try-with-resources closes the consumer, nothing is committed
                    }
                }
                if (strategy != Strategy.COMMIT_BEFORE_PROCESSING) {
                    consumer.commitSync();   // commits the position after the batch: "these are done"
                }
            }
        }
        return processed;
    }

    // ------------------------------------------------------------------ 2. auto-commit timing

    private static void autoCommit(Args args) {
        System.out.println("""

                2. enable.auto.commit=true commits the offsets returned by the previous poll() inside the next poll(), once
                   auto.commit.interval.ms (5000) has elapsed since the last commit. So the committed offset trails the position:
                """);
        String group = "offsets-autocommit-" + System.nanoTime();
        Properties props = Env.consumer(group, "offsets-autocommit");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        args.applyOverrides(props);
        var table = new Table("t (s)", "position (sum over partitions)", "committed (sum)", "uncommitted = redelivered on crash");
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long start = System.currentTimeMillis();
            long nextSample = start;
            while (System.currentTimeMillis() - start < 12_000) {
                consumer.poll(Duration.ofMillis(100));   // 100 records per poll, a "slow" handler of ~100 ms per poll
                Topics.sleep(100);
                if (System.currentTimeMillis() >= nextSample && !consumer.assignment().isEmpty()) {
                    nextSample += 1000;
                    long position = 0, committed = 0;
                    Map<TopicPartition, OffsetAndMetadata> c = consumer.committed(consumer.assignment());
                    for (TopicPartition tp : consumer.assignment()) {
                        position += consumer.position(tp);
                        committed += c.get(tp) == null ? 0 : c.get(tp).offset();
                    }
                    table.row((System.currentTimeMillis() - start) / 1000, position, committed, position - committed);
                }
            }
            // close() with auto-commit on performs a final synchronous commit: a clean shutdown loses nothing.
        }
        table.print("position vs committed offset over 12 s");
        System.out.println("""
                  auto-commit is at-least-once as long as processing happens synchronously inside the poll loop; a crash
                  redelivers up to auto.commit.interval.ms worth of records. It becomes at-most-once the moment you hand
                  records to another thread and keep polling: the offsets get committed before the work is done.
                  a clean close() commits, so orderly shutdowns are exact; only crashes replay.
                """);
    }

    // ------------------------------------------------------------------ 3. auto.offset.reset

    private static void offsetReset(Args args, int records) {
        System.out.println("\n3. auto.offset.reset decides where a group with NO committed offsets starts (it does nothing once offsets exist):\n");
        var table = new Table("auto.offset.reset", "first poll returned", "position after");
        for (String reset : List.of("earliest", "latest", "none")) {
            Properties props = Env.consumer("offsets-reset-" + reset + "-" + System.nanoTime(), "offsets-reset-" + reset);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
            args.applyOverrides(props);
            try (var consumer = new KafkaConsumer<String, String>(props)) {
                consumer.subscribe(List.of(TOPIC));
                long got = 0;
                long deadline = System.currentTimeMillis() + 5000;
                while (got == 0 && System.currentTimeMillis() < deadline) {
                    got = consumer.poll(Duration.ofMillis(500)).count();
                }
                long position = 0;
                for (TopicPartition tp : consumer.assignment()) {
                    position += consumer.position(tp);
                }
                table.row(reset, got + " records", position + (position == records ? " (= end of topic)" : ""));
            } catch (NoOffsetForPartitionException e) {
                table.row(reset, "NoOffsetForPartitionException", "-");
            }
        }
        table.print("");
        System.out.println("  none is the strict choice for pipelines that must never silently skip or replay: fail, and let a human seek.\n");
    }

    // ------------------------------------------------------------------ 4. seeking

    private static void seeking(Args args, int records) {
        System.out.println("4. seeking: offsets are just numbers, and you may set them.\n");
        Properties props = Env.consumer("offsets-seek-" + System.nanoTime(), "offsets-seek");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        args.applyOverrides(props);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }
            Set<TopicPartition> tps = consumer.assignment();
            var table = new Table("action", "records readable from there");

            consumer.seekToBeginning(tps);
            table.row("seekToBeginning", countToEnd(consumer));

            Map<TopicPartition, Long> end = consumer.endOffsets(tps);
            end.forEach((tp, off) -> consumer.seek(tp, Math.max(0, off - 100)));
            table.row("seek(endOffset - 100) on each partition", countToEnd(consumer));

            for (var when : List.of(Map.entry("one hour ago", Instant.now().minusSeconds(3600)), Map.entry("now", Instant.now()))) {
                var query = new HashMap<TopicPartition, Long>();
                tps.forEach(tp -> query.put(tp, when.getValue().toEpochMilli()));
                // offsetsForTimes = the first offset whose record timestamp is >= the given time (null when none is)
                Map<TopicPartition, OffsetAndTimestamp> byTime = consumer.offsetsForTimes(query);
                byTime.forEach((tp, ot) -> consumer.seek(tp, ot == null ? end.get(tp) : ot.offset()));
                table.row("offsetsForTimes(" + when.getKey() + ") then seek", countToEnd(consumer));
            }

            // Committing a chosen offset is how you "rewind" a whole group from the outside as well:
            var rewind = new TreeMap<TopicPartition, OffsetAndMetadata>((a, b) -> a.partition() - b.partition());
            tps.forEach(tp -> rewind.put(tp, new OffsetAndMetadata(0)));
            consumer.commitSync(rewind);
            table.row("commitSync(offset 0 for every partition)", "group now restarts from 0 (see also: kafka-consumer-groups --reset-offsets)");
            table.print("");

            MetricsReport.print("commit metrics of this consumer", consumer.metrics(), MetricsReport.CONSUMER_COORDINATOR,
                    "commit-latency-avg", "commit-rate", "commit-total");
        }
        System.out.printf("  (%d records in the topic)%n", records);
    }

    private static long countToEnd(KafkaConsumer<String, String> consumer) {
        long n = 0;
        int idle = 0;
        while (idle < 3) {
            var batch = consumer.poll(Duration.ofMillis(300));
            if (batch.isEmpty()) {
                idle++;
            } else {
                idle = 0;   // three CONSECUTIVE empty polls mean "end of topic"; the first poll after a seek is
                n += batch.count();   // routinely empty while the fetcher re-issues its fetches
            }
        }
        return n;
    }
}
