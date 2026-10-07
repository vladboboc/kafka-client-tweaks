package io.kafkatweaks.producer;

import io.kafkatweaks.Demo;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.producer.recipe.ExactlyOnceProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Chapter 06: transactions and exactly-once. Measures {@link ExactlyOnceProcessor}; everything else in this file is
 * measurement.
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

    private static final Logger log = LoggerFactory.getLogger(ProducerTransactionsDemo.class);
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
        Knobs.logProducer(txnProducerProps(args, "demo-knobs"),
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ProducerConfig.ACKS_CONFIG, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        Knobs.logConsumer(consumerProps(args, "demo-knobs", "read_committed"),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);

        atomicWrites(args);
        commitCost(args, size);
        exactlyOnce(args, records, size);
    }

    // ------------------------------------------------------------------ 1. atomic writes

    private static void atomicWrites(Args args) {
        log.info("1. atomic writes across partitions");
        try (var producer = new KafkaProducer<String, String>(txnProducerProps(args, "atomic-writer"))) {
            producer.initTransactions();

            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(OUT, "aborted-" + i, "this record will never be read by read_committed consumers"));
            }
            producer.abortTransaction();
            log.info("sent 10 records over 3 partitions, then abortTransaction()");

            var committed = new ArrayList<ProducerRecord<String, String>>();
            for (int i = 0; i < 10; i++) {
                committed.add(new ProducerRecord<>(OUT, "committed-" + i, "visible"));
            }
            ExactlyOnceProcessor.sendAtomically(producer, committed);   // <- the recipe under test
            log.info("sent 10 records over 3 partitions, then commitTransaction()");
        }

        var table = new Table("isolation.level", "records seen", "keys");
        for (String isolation : List.of("read_uncommitted", "read_committed")) {
            Set<String> keys = new HashSet<>();
            long n = drain(consumerProps(args, "atomic-" + isolation, isolation), OUT, r -> keys.add(r.key().replaceAll("-\\d+$", "")));
            table.row(isolation, n, String.join(",", keys));
        }
        log.info("what consumers see (both read the topic from the beginning)\n{}", table);
        log.info("""
                aborted records are physically in the log (read_uncommitted returns them); read_committed consumers skip
                  them using the transaction markers the coordinator wrote when the transaction ended.
                  the default isolation.level is read_uncommitted: a consumer that must not see aborted data has to opt in.""");
    }

    // ------------------------------------------------------------------ 2. commit cost

    private static void commitCost(Args args, int size) {
        log.info("2. what a transaction costs: commit = round trips to the transaction coordinator + markers on every partition");
        var table = new Table("records per transaction", "records/s", "commits/s");
        int total = 3000;
        for (int perTxn : new int[] {1, 10, 100, 1000}) {
            try (var producer = new KafkaProducer<String, String>(txnProducerProps(args, "cost-" + perTxn))) {
                producer.initTransactions();
                var watch = Stopwatch.start();
                int commits = 0;
                for (int i = 0; i < total; i += perTxn) {
                    var records = new ArrayList<ProducerRecord<String, String>>(perTxn);
                    for (int j = 0; j < perTxn; j++) {
                        records.add(new ProducerRecord<>(OUT, null, Payloads.json(i + j, size)));
                    }
                    ExactlyOnceProcessor.sendAtomically(producer, records);
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
        log.info("{} records of {} bytes\n{}", total, size, table);
        log.info("a transaction per record is the classic mistake; batch by time (e.g. every 100 ms) or by poll() and it is nearly free.");
    }

    // ------------------------------------------------------------------ 3. exactly-once consume-transform-produce

    private static void exactlyOnce(Args args, int records, int size) throws Exception {
        log.info("3. exactly-once consume-transform-produce: {} -> upper-case -> {}", IN, OUT);
        try (var topics = new Topics()) {
            topics.recreate(OUT, 3);
        }
        try (var seed = new KafkaProducer<String, String>(plainProducerProps(args, "seed"))) {
            for (int i = 0; i < records; i++) {
                seed.send(new ProducerRecord<>(IN, "in-" + i, Payloads.json(i, size)));
            }
            seed.flush();
        }
        log.info("seeded {} input records", records);

        // Run 1 processes about half, then "crashes" in the middle of a transaction (no commit, no close).
        var first = new Processor(args, "processor-1", records / 2);
        int processedBeforeCrash = first.run();
        log.info("processor-1 processed {} records and crashed mid-transaction (in-flight batch neither committed nor aborted)", processedBeforeCrash);

        // Run 2 uses the SAME transactional.id: initTransactions() fences processor-1 and aborts its open transaction.
        var second = new Processor(args, "processor-2", Integer.MAX_VALUE);
        int processedAfterRestart = second.run();
        log.info("processor-2 (same transactional.id) resumed from the last committed offsets and processed {} records", processedAfterRestart);

        // Zombie fencing: processor-1's producer is still inside its half-finished transaction. Whatever it tries
        // next is rejected, because processor-2's initTransactions() bumped the epoch of the transactional.id.
        try {
            first.producer.send(new ProducerRecord<>(OUT, "zombie", "should never land"));
            first.producer.commitTransaction();
            log.warn("unexpected: zombie producer was not fenced");
        } catch (KafkaException e) {
            Throwable c = e.getCause() instanceof ProducerFencedException f ? f : e;
            log.info("processor-1's producer is a zombie now: {}: {}", c.getClass().getSimpleName(), firstLine(c.getMessage()));
        } finally {
            first.producer.close(Duration.ZERO);
        }

        var seen = new HashMap<String, Integer>();
        long n = drain(consumerProps(args, "verify", "read_committed"), OUT, r -> seen.merge(r.key(), 1, Integer::sum));
        long duplicates = seen.values().stream().filter(c -> c > 1).count();
        var result = new Table("output records (read_committed)", "distinct input keys", "keys seen twice", "input records")
                .row(n, seen.size(), duplicates, records);
        log.info("result\n{}", result);
        log.info("""
                the half-finished transaction of the crashed processor was aborted by the coordinator when the successor
                  called initTransactions(); its offsets were never committed, so the successor re-read those records and
                  produced them again inside a new transaction. Output = input, exactly once.
                  without transactions (offset commit separate from produce) the same crash yields duplicates in the output,
                  or, with commit-before-produce, lost records. That is chapter 08's territory.""");
    }

    /** One consume-transform-produce instance. Stops (without committing) after {@code crashAfter} records. */
    private static final class Processor {
        final KafkaProducer<String, String> producer;
        final KafkaConsumer<String, String> consumer;
        final int crashAfter;
        final String name;
        int processed;   // records handed to the transform so far; the transform lambda counts them

        Processor(Args args, String name, int crashAfter) {
            this.name = name;
            this.crashAfter = crashAfter;
            // The SAME transactional.id across restarts: that is what lets processor-2 fence processor-1.
            this.producer = new KafkaProducer<>(args.applyOverrides(
                    ExactlyOnceProcessor.transactional(Env.producer("txn-" + name), "tweaks-txn-processor", Duration.ofSeconds(30))));
            this.consumer = new KafkaConsumer<>(ExactlyOnceProcessor.readCommitted(consumerProps(args, name, "read_committed"), "txn-processor"));
        }

        int run() {
            producer.initTransactions();   // also aborts any transaction a previous incarnation of this transactional.id left open
            consumer.subscribe(List.of(IN));
            // No separate "wait for the assignment" loop: the poll() that completes the group join can already
            // return records, and a loop that ignores poll() results silently skips them (their offsets are
            // then committed by the next transaction as if they had been processed). Every poll result counts.
            boolean announced = false;
            int idlePolls = 0;
            int txn = 0;
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                if (!announced && !consumer.assignment().isEmpty()) {
                    log.info("{} owns partitions {}", name, consumer.assignment().stream()
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
                            log.info("{}: idle, input lag {}, assignment {}", name, lag, consumer.assignment());
                        }
                    }
                    continue;
                }
                idlePolls = 0;
                txn++;
                try {
                    // The recipe under test: send the transformed batch and its input offsets in one transaction.
                    boolean committed = ExactlyOnceProcessor.processBatch(consumer, producer, batch, r -> {
                        if (++processed >= crashAfter) {
                            throw new SimulatedCrash(r.partition(), r.offset());
                        }
                        return new ProducerRecord<>(OUT, r.key(), r.value().toUpperCase());
                    });
                    log.info("{} txn #{}: {} -> {}", name, txn, ranges(batch, null),
                            committed ? "committed" : "aborted, the batch will be read again");
                } catch (SimulatedCrash crash) {
                    // Not a KafkaException, so processBatch neither committed nor aborted: the transaction is left open,
                    // exactly what a JVM crash leaves behind. The offsets were never committed (they only travel inside
                    // the transaction). The consumer is closed cleanly only so that the group does not have to wait
                    // session.timeout.ms (45 s) for a dead member before the successor gets partitions.
                    // The producer object stays alive on purpose so the fencing check below can use it.
                    log.info("{} txn #{}: {} -> CRASH before commit (will be aborted)", name, txn, ranges(batch, crash));
                    consumer.close();
                    return processed;
                }
            }
            consumer.close();
            producer.close();
            return processed;
        }

        /**
         * "p0[0..499] p2[0..152]": which input offsets a batch covered. For the crashed batch ({@code crash} not null)
         * the list ends at the record the transform threw on: partitions are processed in batch order, so the ones
         * after the crash partition were never reached.
         */
        private static String ranges(ConsumerRecords<String, String> batch, SimulatedCrash crash) {
            var sb = new StringBuilder();
            for (TopicPartition tp : batch.partitions()) {
                var recs = batch.records(tp);
                long first = recs.getFirst().offset();
                boolean crashedHere = crash != null && tp.partition() == crash.partition;
                long last = crashedHere ? crash.offset : recs.getLast().offset();
                sb.append("p").append(tp.partition()).append('[').append(first).append("..").append(last).append("] ");
                if (crashedHere) {
                    break;
                }
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

    /** The demo's stand-in for a JVM crash: thrown from the transform, so the transaction is left open. */
    private static final class SimulatedCrash extends RuntimeException {
        final int partition;
        final long offset;

        SimulatedCrash(int partition, long offset) {
            super("simulated crash at input p" + partition + " offset " + offset, null, false, false);
            this.partition = partition;
            this.offset = offset;
        }
    }

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
        return args.applyOverrides(ExactlyOnceProcessor.transactional(Env.producer("txn-" + clientId), "tweaks-txn-" + clientId, Duration.ofSeconds(30)));
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
