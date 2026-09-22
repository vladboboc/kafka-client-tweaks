package io.kafkatweaks.spring.txn;

import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Seed;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.ClientCapture;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.txn.recipe.OrderTransfer;
import io.kafkatweaks.spring.txn.recipe.TxnRecipe;
import io.kafkatweaks.spring.txn.recipe.UppercaseProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;

/**
 * Chapter 19: transactions the Spring way. Measures {@link TxnRecipe}, {@link OrderTransfer} and {@link UppercaseProcessor};
 * everything else in this file is measurement.
 * <ol>
 *   <li>{@code executeInTransaction}: atomic writes, aborted vs committed, read_uncommitted vs read_committed</li>
 *   <li>a transactional template refuses a plain send() unless {@code allowNonTransactional}</li>
 *   <li>{@code @Transactional} on a service method (the KafkaTransactionManager is a PlatformTransactionManager)</li>
 *   <li>the consume-transform-produce processor of chapter 06 as a record listener (a transaction per record) and
 *       as a batch listener (a transaction per poll) that crashes mid-batch: exactly-once output</li>
 * </ol>
 * <pre>
 *   records=1200   input records for part 4
 *   sample=200     records the per-record listener processes before it is stopped (each one is a transaction)
 *   crash=800      the record at which the batch listener throws (once)
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-transactions")
public class TransactionsDemo {

    @Bean
    ApplicationRunner springTransactions(DemoSupport support, KafkaTemplate<String, String> template, ProducerFactory<String, String> producerFactory,
                                         OrderTransfer transfer, TxnProbe probe, ClientCapture capture, KafkaTransactionManager<?, ?> transactionManager) {
        return support.demo("spring-transactions", args -> {
            long records = args.getLong("records", 1200);
            long sample = args.getLong("sample", 200);
            probe.crashAtRecord(args.getLong("crash", 800));
            System.out.printf("KafkaTransactionManager bean present: %s (auto-configured because spring.kafka.producer.transaction-id-prefix is set); template.isTransactional()=%s%n%n",
                    transactionManager.getClass().getSimpleName(), template.isTransactional());

            // ---- 1. executeInTransaction ------------------------------------------------------------------------
            try (var topics = new Topics()) {
                topics.recreate(TopicsConfig.TXN_OUT, 3);   // before any transactional producer exists (see part 4)
            }
            try {
                template.executeInTransaction(ops -> {
                    for (int i = 0; i < 10; i++) {
                        ops.send(TopicsConfig.TXN_OUT, "aborted-" + i, "will be rolled back");
                    }
                    ops.flush();   // make sure the records reach the log before the abort (otherwise they die in the accumulator)
                    throw new IllegalStateException("abort by script");
                });
            } catch (IllegalStateException expected) {
                System.out.println("transaction 1: 10 sends, then " + expected.getMessage() + " -> rolled back (abort markers written)");
            }
            var committed10 = new ArrayList<ProducerRecord<String, String>>();
            for (int i = 0; i < 10; i++) {
                committed10.add(new ProducerRecord<>(TopicsConfig.TXN_OUT, "committed-" + i, "visible to everyone"));
            }
            TxnRecipe.sendAll(template, committed10);   // <- the recipe under test
            System.out.println("transaction 2: 10 sends, callback returned normally -> committed");
            var atomic = new Table("isolation.level", "records seen", "keys");
            for (String isolation : List.of("read_uncommitted", "read_committed")) {
                List<ConsumerRecord<String, String>> seen = drain(TopicsConfig.TXN_OUT, isolation);
                Set<String> prefixes = new TreeSet<>();
                seen.forEach(r -> prefixes.add(r.key().substring(0, r.key().indexOf('-'))));
                atomic.row(isolation, seen.size(), String.join(",", prefixes));
            }
            atomic.print("1. executeInTransaction: one transaction aborted, one committed, the same topic read with both isolation levels");

            // ---- 2. a plain send() on a transactional template ---------------------------------------------------
            var plain = new Table("template", "allowNonTransactional", "send() outside a transaction");
            try {
                template.send(TopicsConfig.TXN_OUT, "plain", "no transaction around me").get();
                plain.row("kafkaTemplate (Boot)", false, "succeeded (unexpected)");
            } catch (IllegalStateException e) {
                plain.row("kafkaTemplate (Boot)", false, "IllegalStateException: " + e.getMessage().substring(0, Math.min(90, e.getMessage().length())) + "...");
            }
            var lenient = TxnRecipe.nonTransactionalTemplate(producerFactory);   // same transactional factory, not a bean
            lenient.send(TopicsConfig.TXN_OUT, "plain", "sent by a non-transactional producer of the same factory").get();
            plain.row("new KafkaTemplate(sameFactory) + setAllowNonTransactional(true)", true, "succeeded: the factory hands out a non-transactional producer for this call");
            plain.print("2. spring.kafka.template.allow-non-transactional (default false): a transactional template protects you from forgetting the transaction");

            // ---- 3. @Transactional -----------------------------------------------------------------------------------
            transfer.transfer("t1", false);
            try {
                transfer.transfer("t2", true);
            } catch (IllegalStateException expected) {
                System.out.println("@Transactional transfer(t2): " + expected.getMessage() + " -> rolled back by the KafkaTransactionManager");
            }
            List<ConsumerRecord<String, String>> committed = drain(TopicsConfig.TXN_OUT, "read_committed");
            long t1 = committed.stream().filter(r -> r.key().startsWith("t1-")).count();
            long t2 = committed.stream().filter(r -> r.key().startsWith("t2-")).count();
            System.out.printf("3. @Transactional: transfer t1 committed %d records, transfer t2 (threw) committed %d; read_committed sees %d records in total%n%n", t1, t2, committed.size());

            // ---- 4. the listeners: consume-transform-produce, exactly once ----------------------------------------
            // spring.txn-out is NOT recreated here: a topic recreated under a live transactional producer leaves it with a
            // stale topic id (UNKNOWN_TOPIC_ID) and its next sendOffsetsToTransaction never returns. Records of this part
            // are told apart from parts 1-3 by their offsets instead.
            long total;
            try (var topics = new Topics()) {
                topics.recreate(TopicsConfig.TXN_IN, 3);
                total = Seed.ensure(topics, TopicsConfig.TXN_IN, 3, records, 200);
                topics.deleteGroup("spring-txn-record");
                topics.deleteGroup("spring-txn-batch");
            }
            var eos = new Table("listener", "records processed", "transactions", "ms", "ms per transaction", "output read_committed", "distinct", "duplicates", "read_uncommitted (incl. aborted)");

            // 4a. one transaction per record: stopped after `sample` records, this is about the price
            Map<TopicPartition, Long> before = endOffsets(TopicsConfig.TXN_OUT);
            var sw = Stopwatch.start();
            support.start("txn-per-record");
            await(probe::recordCalls, Math.min(sample, total), Duration.ofSeconds(90), "txn-per-record");
            double recordMs = sw.elapsedMillis();
            support.stop("txn-per-record");
            // stop() returns when the consumer thread is done or after shutdown-timeout (10 s). With immediate-stop=true (this
            // profile) the thread quits after the current record instead of after the current poll; wait for it to close.
            await(() -> capture.consumers("txn-record").isEmpty() ? 1 : 0, 1, Duration.ofSeconds(30), "txn-per-record consumer closed");
            row(eos, "txn-per-record (stopped after " + Math.min(sample, total) + ")", probe.recordCalls(), probe.recordCalls(), " (one per record)", recordMs, before);

            // 4b. one transaction per poll, one crash in the middle of a batch: the whole topic
            before = endOffsets(TopicsConfig.TXN_OUT);
            sw = Stopwatch.start();
            support.start("txn-per-batch");
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            try (var topics = new Topics()) {
                while (System.nanoTime() < deadline) {
                    // done = every input partition has a committed offset (sent inside a transaction) and no lag is left
                    Map<TopicPartition, Long> lag = topics.lag("spring-txn-batch");
                    if (probe.batchRecords() >= total && lag.size() >= 3 && lag.values().stream().mapToLong(Long::longValue).sum() == 0) {
                        break;
                    }
                    DemoSupport.sleep(200);
                }
            }
            double batchMs = sw.elapsedMillis();
            support.stop("txn-per-batch");
            row(eos, "txn-per-batch (batch=\"true\", all " + total + ")", probe.batchRecords(), probe.batchCalls(), " (one per poll, incl. the rolled-back one)", batchMs, before);

            probe.events().forEach(e -> System.out.println("   " + e));
            eos.print("4. @KafkaListener + KafkaTemplate inside the container's transaction (max.poll.records=500); the batch listener crashes once at record %d".formatted(args.getLong("crash", 800)));
            // NOT "the transactional producers": part 2's setAllowNonTransactional(true) send made this same factory
            // create and cache a producer with transactional.id=null, and it registers with ProducerFactory.Listener
            // exactly like the transactional ones. The ids (factory.<client.id>) do not say which is which.
            System.out.printf("   producers this factory created this run (ClientCapture ids = factory.<client.id>): %s%n", new TreeSet<>(capture.producers().keySet()));
            System.out.println("   one of them is the non-transactional producer part 2 borrowed; a transactional factory hands one out for");
            System.out.println("   allow-non-transactional sends and keeps it until the context closes.");
        });
    }

    private static void row(Table table, String label, long processed, long transactions, String transactionNote, double ms, Map<TopicPartition, Long> since) {
        List<ConsumerRecord<String, String>> committedOut = drainSince(TopicsConfig.TXN_OUT, "read_committed", since);
        List<ConsumerRecord<String, String>> allOut = drainSince(TopicsConfig.TXN_OUT, "read_uncommitted", since);
        Set<String> distinct = new HashSet<>();
        long duplicates = committedOut.stream().filter(r -> !distinct.add(r.key() + "@" + r.value())).count();
        table.row(label, processed, transactions + transactionNote, "%.0f".formatted(ms), "%.1f".formatted(ms / Math.max(1, transactions)),
                committedOut.size(), distinct.size(), duplicates, allOut.size());
    }

    private static Map<TopicPartition, Long> endOffsets(String topic) {
        try (var topics = new Topics()) {
            return topics.endOffsets(topic);
        }
    }

    /** A plain consumer in a fresh group reads a topic with the given isolation level, keeping only records at or after the given offsets. */
    private static List<ConsumerRecord<String, String>> drainSince(String topic, String isolation, Map<TopicPartition, Long> from) {
        return drain(topic, isolation).stream()
                .filter(r -> r.offset() >= from.getOrDefault(new TopicPartition(r.topic(), r.partition()), 0L))
                .toList();
    }

    /**
     * A plain consumer reads the whole topic with the given isolation level, up to the end offsets it sees.
     * It assigns the partitions rather than subscribing, and commits nothing: this runs up to seven times per demo,
     * and a group per call would leave seven empty {@code spring-txn-verify-*} groups behind on every run.
     */
    private static List<ConsumerRecord<String, String>> drain(String topic, String isolation) {
        var props = Env.consumer("spring-txn-verify", "verify-" + isolation);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        var records = new ArrayList<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);   // with read_committed: the last stable offset
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(records::add);
                boolean done = true;
                for (var e : ends.entrySet()) {
                    done &= consumer.position(e.getKey()) >= e.getValue();
                }
                if (done) {
                    break;
                }
            }
        }
        return records;
    }

    private static void await(LongSupplier value, long target, Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (value.getAsLong() < target) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(what + ": " + value.getAsLong() + " of " + target + " within " + timeout);
            }
            DemoSupport.sleep(50);
        }
    }
}
