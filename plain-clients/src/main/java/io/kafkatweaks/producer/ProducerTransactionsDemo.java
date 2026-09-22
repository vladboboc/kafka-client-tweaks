package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Chapter 06: transactions and exactly-once.
 * <ol>
 *   <li>atomic multi-partition writes: abort vs commit, seen through read_committed and read_uncommitted consumers</li>
 *   <li>what a commit costs: throughput vs records per transaction</li>
 *   <li>consume-transform-produce with {@code sendOffsetsToTransaction}: a "crash" mid-way, a restart with the same
 *       transactional.id, zombie fencing, and an output with exactly one copy of every input record</li>
 * </ol>
 * <pre>
 *   records=2000     input records for part 3
 *   size=200
 * </pre>
 */
public final class ProducerTransactionsDemo implements Demo {

    private static final String IN = "tweaks.txn-in";
    private static final String OUT = "tweaks.txn-out";

    @Override
    public void run(Args args) throws Exception {
        int records = args.getInt("records", 2000);
        int size = args.getInt("size", 200);

        try (var topics = new Topics()) {
            topics.recreate(IN, 3);
            topics.recreate(OUT, 3);
            topics.deleteGroup("txn-processor");
        }
        Knobs.printProducer(txnProducerProps(args, "demo-knobs"),
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ProducerConfig.ACKS_CONFIG, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        Knobs.printConsumer(consumerProps(args, "demo-knobs", "read_committed"),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);

        atomicWrites(args);
        commitCost(args, size);
        exactlyOnce(args, records, size);
    }

    // ------------------------------------------------------------------ 1. atomic writes

    private static void atomicWrites(Args args) {
        System.out.println("\n1. atomic writes across partitions\n");
        try (var producer = new KafkaProducer<String, String>(txnProducerProps(args, "atomic-writer"))) {
            producer.initTransactions();

            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(OUT, "aborted-" + i, "this record will never be read by read_committed consumers"));
            }
            producer.abortTransaction();
            System.out.println("sent 10 records over 3 partitions, then abortTransaction()");

            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(OUT, "committed-" + i, "visible"));
            }
            producer.commitTransaction();
            System.out.println("sent 10 records over 3 partitions, then commitTransaction()");
        }

        var table = new Table("isolation.level", "records seen", "keys");
        for (String isolation : List.of("read_uncommitted", "read_committed")) {
            Set<String> keys = new HashSet<>();
            long n = drain(consumerProps(args, "atomic-" + isolation, isolation), OUT, r -> keys.add(r.key().replaceAll("-\\d+$", "")));
            table.row(isolation, n, String.join(",", keys));
        }
        table.print("what consumers see (both read the topic from the beginning)");
        System.out.println("""
                aborted records are physically in the log (read_uncommitted returns them); read_committed consumers skip
                them using the transaction markers the coordinator wrote when the transaction ended.
                the default isolation.level is read_uncommitted: a consumer that must not see aborted data has to opt in.
                """);
    }

    // ------------------------------------------------------------------ 2. commit cost

    private static void commitCost(Args args, int size) {
        System.out.println("\n2. what a transaction costs: commit = round trips to the transaction coordinator + markers on every partition\n");
        var table = new Table("records per transaction", "records/s", "commits/s");
        int total = 3000;
        for (int perTxn : new int[] {1, 10, 100, 1000}) {
            try (var producer = new KafkaProducer<String, String>(txnProducerProps(args, "cost-" + perTxn))) {
                producer.initTransactions();
                var watch = Stopwatch.start();
                int commits = 0;
                for (int i = 0; i < total; i += perTxn) {
                    producer.beginTransaction();
                    for (int j = 0; j < perTxn; j++) {
                        producer.send(new ProducerRecord<>(OUT, null, Payloads.json(i + j, size)));
                    }
                    producer.commitTransaction();
                    commits++;
                }
                table.row(perTxn, watch.rate(total), watch.rate(commits));
            }
        }
        try (var producer = new KafkaProducer<String, String>(plainProducerProps(args, "cost-none"))) {
            var watch = Stopwatch.start();
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(OUT, null, Payloads.json(i, size)));
            }
            producer.flush();
            table.row("no transaction (idempotent)", watch.rate(total), 0);
        }
        table.print("%d records of %d bytes".formatted(total, size));
        System.out.println("a transaction per record is the classic mistake; batch by time (e.g. every 100 ms) or by poll() and it is nearly free.\n");
    }

    // ------------------------------------------------------------------ 3. exactly-once consume-transform-produce

    private static void exactlyOnce(Args args, int records, int size) throws Exception {
        System.out.printf("%n3. exactly-once consume-transform-produce: %s -> upper-case -> %s%n%n", IN, OUT);
        try (var topics = new Topics()) {
            topics.recreate(OUT, 3);
        }
        try (var seed = new KafkaProducer<String, String>(plainProducerProps(args, "seed"))) {
            for (int i = 0; i < records; i++) {
                seed.send(new ProducerRecord<>(IN, "in-" + i, Payloads.json(i, size)));
            }
            seed.flush();
        }
        System.out.printf("seeded %d input records%n", records);

        // Run 1 processes about half, then "crashes" in the middle of a transaction (no commit, no close).
        var first = new Processor(args, "processor-1", records / 2);
        int processedBeforeCrash = first.run();
        System.out.printf("processor-1 processed %d records and crashed mid-transaction (in-flight batch neither committed nor aborted)%n", processedBeforeCrash);

        // Run 2 uses the SAME transactional.id: initTransactions() fences processor-1 and aborts its open transaction.
        var second = new Processor(args, "processor-2", Integer.MAX_VALUE);
        int processedAfterRestart = second.run();
        System.out.printf("processor-2 (same transactional.id) resumed from the last committed offsets and processed %d records%n", processedAfterRestart);

        // Zombie fencing: processor-1's producer is still inside its half-finished transaction. Whatever it tries
        // next is rejected, because processor-2's initTransactions() bumped the epoch of the transactional.id.
        try {
            first.producer.send(new ProducerRecord<>(OUT, "zombie", "should never land"));
            first.producer.commitTransaction();
            System.out.println("unexpected: zombie producer was not fenced");
        } catch (KafkaException e) {
            Throwable c = e.getCause() instanceof ProducerFencedException f ? f : e;
            System.out.printf("processor-1's producer is a zombie now: %s: %s%n", c.getClass().getSimpleName(), firstLine(c.getMessage()));
        } finally {
            first.producer.close(Duration.ZERO);
        }

        var seen = new HashMap<String, Integer>();
        long n = drain(consumerProps(args, "verify", "read_committed"), OUT, r -> seen.merge(r.key(), 1, Integer::sum));
        long duplicates = seen.values().stream().filter(c -> c > 1).count();
        new Table("output records (read_committed)", "distinct input keys", "keys seen twice", "input records")
                .row(n, seen.size(), duplicates, records)
                .print("result");
        System.out.println("""
                the half-finished transaction of the crashed processor was aborted by the coordinator when the successor
                called initTransactions(); its offsets were never committed, so the successor re-read those records and
                produced them again inside a new transaction. Output = input, exactly once.
                without transactions (offset commit separate from produce) the same crash yields duplicates in the output,
                or, with commit-before-produce, lost records. That is chapter 08's territory.
                """);
    }

    /** One consume-transform-produce instance. Stops (without committing) after {@code crashAfter} records. */
    private static final class Processor {
        final KafkaProducer<String, String> producer;
        final KafkaConsumer<String, String> consumer;
        final int crashAfter;
        final String name;

        Processor(Args args, String name, int crashAfter) {
            this.name = name;
            this.crashAfter = crashAfter;
            var pp = txnProducerProps(args, name);
            pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tweaks-txn-processor");   // the SAME id across restarts
            this.producer = new KafkaProducer<>(pp);
            var cp = consumerProps(args, name, "read_committed");
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-processor");
            cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // offsets go through the transaction
            this.consumer = new KafkaConsumer<>(cp);
        }

        int run() {
            producer.initTransactions();   // also aborts any transaction a previous incarnation of this transactional.id left open
            consumer.subscribe(List.of(IN));
            // No separate "wait for the assignment" loop: the poll() that completes the group join can already
            // return records, and a loop that ignores poll() results silently skips them (their offsets are
            // then committed by the next transaction as if they had been processed). Every poll result counts.
            boolean announced = false;
            int processed = 0;
            int idlePolls = 0;
            int txn = 0;
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                if (!announced && !consumer.assignment().isEmpty()) {
                    System.out.printf("%s owns partitions %s%n", name, consumer.assignment().stream()
                            .map(tp -> String.valueOf(tp.partition())).sorted().toList());
                    announced = true;
                }
                if (batch.isEmpty()) {
                    if (!announced) {
                        continue;   // still joining the group
                    }
                    // Stop only when the whole group has nothing left, not just this member's current partitions:
                    // a rebalance may still be moving partitions over from the crashed predecessor.
                    if (++idlePolls >= 4) {
                        long lag = inputLag();
                        if (lag == 0) {
                            break;
                        }
                        if (idlePolls % 10 == 0) {
                            System.out.printf("%s: idle, input lag %d, assignment %s%n", name, lag, consumer.assignment());
                        }
                    }
                    continue;
                }
                idlePolls = 0;
                txn++;
                producer.beginTransaction();
                var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (ConsumerRecord<String, String> r : batch) {
                    producer.send(new ProducerRecord<>(OUT, r.key(), r.value().toUpperCase()));
                    offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    processed++;
                    if (processed >= crashAfter) {
                        // Simulated crash: return without commitTransaction()/abortTransaction(). Offsets were never
                        // committed (they only travel inside the transaction), so this is exactly what a JVM crash
                        // leaves behind. The consumer is closed cleanly only so that the group does not have to wait
                        // session.timeout.ms (45 s) for a dead member before the successor gets partitions.
                        // The producer object stays alive on purpose so the fencing check below can use it.
                        System.out.printf("%s txn #%d: %s -> CRASH before commit (will be aborted)%n", name, txn, ranges(batch, r.offset()));
                        consumer.close();
                        return processed;
                    }
                }
                // Offsets are committed AS PART OF the transaction: they become visible together with the output records.
                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                producer.commitTransaction();
                System.out.printf("%s txn #%d: %s -> committed%n", name, txn, ranges(batch, Long.MAX_VALUE));
            }
            consumer.close();
            producer.close();
            return processed;
        }

        /** "p0[0..499] p2[0..152]": which input offsets a batch covered, capped at {@code upTo} for the crashed batch. */
        private static String ranges(ConsumerRecords<String, String> batch, long upTo) {
            var sb = new StringBuilder();
            for (TopicPartition tp : batch.partitions()) {
                var recs = batch.records(tp);
                long first = recs.getFirst().offset();
                long last = Math.min(recs.getLast().offset(), upTo);
                if (last < first) {
                    continue;
                }
                sb.append("p").append(tp.partition()).append('[').append(first).append("..").append(last).append("] ");
            }
            return sb.toString().trim();
        }

        /** Records in the input topic that no committed offset of the group covers yet. */
        private static long inputLag() {
            try (var topics = new Topics()) {
                Map<TopicPartition, Long> ends = topics.endOffsets(IN);
                Map<TopicPartition, Long> lag = topics.lag("txn-processor");
                long total = 0;
                for (var e : ends.entrySet()) {
                    // partitions without a committed offset count in full
                    total += lag.containsKey(e.getKey()) ? lag.get(e.getKey()) : e.getValue();
                }
                return total;
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private interface RecordVisitor {
        void visit(ConsumerRecord<String, String> record);
    }

    private static long drain(Properties props, String topic, RecordVisitor visitor) {
        long n = 0;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            int idle = 0;
            while (idle < 6) {
                var batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    idle++;
                    continue;
                }
                idle = 0;
                for (var r : batch) {
                    visitor.visit(r);
                    n++;
                }
            }
        }
        return n;
    }

    private static Properties txnProducerProps(Args args, String clientId) {
        var p = Env.producer("txn-" + clientId);
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tweaks-txn-" + clientId);
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");
        return args.applyOverrides(p);
    }

    private static Properties plainProducerProps(Args args, String clientId) {
        return args.applyOverrides(Env.producer("txn-" + clientId));
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }

    private static Properties consumerProps(Args args, String clientId, String isolation) {
        var p = Env.consumer("txn-reader-" + clientId + "-" + System.nanoTime(), "txn-" + clientId);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        return args.applyOverrides(p);
    }
}
