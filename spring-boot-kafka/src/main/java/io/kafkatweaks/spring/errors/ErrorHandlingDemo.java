package io.kafkatweaks.spring.errors;

import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Order;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.errors.recipe.ErrorHandlingRecipe;
import io.kafkatweaks.spring.errors.recipe.OrderListeners;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

/**
 * Chapter 18: what happens when the listener throws. Measures {@link ErrorHandlingRecipe} and {@link OrderListeners}
 * (with {@link ScriptedOrderHandler} as the service behind them); everything else in this file is measurement.
 * <ol>
 *   <li>blocking retries: {@code DefaultErrorHandler} with an exponential back-off, non-retryable exceptions,
 *       a poison pill caught by {@code ErrorHandlingDeserializer}, everything unrecoverable published to
 *       {@code spring.errors-dlt} with the {@code kafka_dlt-*} headers</li>
 *   <li>non-blocking retries: {@code @RetryableTopic} re-publishes the failed record to {@code -retry-1000},
 *       {@code -retry-2000}, {@code -retry-4000} and finally {@code -dlt}, while the partition moves on</li>
 *   <li>the price of blocking: how long an innocent record waits behind a failing one</li>
 * </ol>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-error-handling")
public class ErrorHandlingDemo {

    @Bean
    ApplicationRunner springErrorHandling(DemoSupport support, KafkaTemplate<String, Object> template,
                                          ProducerFactory<String, Object> producerFactory, ScriptedOrderHandler handler) {
        return support.demo("spring-error-handling", args -> {
            try (var topics = new Topics()) {
                topics.recreate(TopicsConfig.ERRORS, 3);
                topics.recreate(TopicsConfig.ERRORS_DLT, 3);
                topics.ensure(TopicsConfig.RETRYABLE, 3);
                topics.deleteGroup("spring-errors-blocking");
                topics.deleteGroup("spring-errors-retryable");
            }
            handler.markStart();

            // ---- 1. blocking retries + DLT ---------------------------------------------------------------------
            List<String> keys = List.of("ok-1", "ok-2", "flaky2-3", "fatal-4", "ok-5", "flaky9-6", "ok-7");
            for (String key : keys) {
                template.send(TopicsConfig.ERRORS, key, Order.sample(key.hashCode() & 0xffff)).get(10, TimeUnit.SECONDS);
            }
            // The poison pill: bytes that are not JSON, sent with a template that does not try to make them JSON.
            @SuppressWarnings({"unchecked", "rawtypes"})
            var raw = new KafkaTemplate<String, byte[]>((ProducerFactory) producerFactory,
                    Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class, ProducerConfig.CLIENT_ID_CONFIG, "spring-errors-poison"));
            try {
                raw.send(TopicsConfig.ERRORS, "poison-8", "{not json".getBytes(StandardCharsets.UTF_8)).get(10, TimeUnit.SECONDS);
            } finally {
                raw.destroy();
            }
            System.out.println("sent to spring.errors: " + keys + " + poison-8 (value '{not json')");

            support.start("errors-blocking");
            // ok x4 = 4 calls, flaky2 = 3 calls, fatal = 1 call, flaky9 = 4 calls (3 retries); the poison pill never reaches the listener.
            await(() -> handler.blocking().size(), 12, Duration.ofSeconds(30), "blocking listener calls");
            List<ConsumerRecord<String, byte[]>> dead = readDlt(3, Duration.ofSeconds(20));
            support.stop("errors-blocking");

            var timeline = new Table("t ms", "partition", "key", "attempt", "kafka_deliveryAttempt header", "ms since send", "listener");
            handler.blocking().stream().sorted(Comparator.comparingLong(ScriptedOrderHandler.Attempt::tMs)).forEach(a ->
                    timeline.row(a.tMs(), a.partition(), a.key() + "  (" + FailureScript.describe(a.key()) + ")", a.attempt(), a.springAttempt(), a.sinceSendMs(), a.outcome()));
            timeline.print("1. blocking: DefaultErrorHandler(ExponentialBackOffWithMaxRetries(3): 200, 400, 800 ms), IllegalArgumentException not retryable");

            var dlt = new Table("DLT key", "value", "kafka_dlt-exception-fqcn", "kafka_dlt-exception-cause-fqcn", "original topic-partition@offset");
            for (ConsumerRecord<String, byte[]> r : dead) {
                String value = new String(r.value(), StandardCharsets.UTF_8);
                dlt.row(r.key(), value.length() > 34 ? value.substring(0, 34) + "..." : value, simpleName(header(r, KafkaHeaders.DLT_EXCEPTION_FQCN)),
                        simpleName(header(r, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)),
                        header(r, KafkaHeaders.DLT_ORIGINAL_TOPIC) + "-" + intHeader(r, KafkaHeaders.DLT_ORIGINAL_PARTITION) + "@" + longHeader(r, KafkaHeaders.DLT_ORIGINAL_OFFSET));
            }
            dlt.print("   spring.errors-dlt (%d records): same partition as the original, original value bytes, the exception in headers".formatted(dead.size()));

            // ---- 2. non-blocking retries ------------------------------------------------------------------------
            List<String> retryIds = support.registry().getListenerContainerIds().stream().filter(id -> id.startsWith("errors-retryable")).sorted().toList();
            support.start(retryIds.toArray(String[]::new));
            List<String> retryKeys = List.of("ok-1", "flaky2-2", "flaky9-3", "ok-4");
            for (String key : retryKeys) {
                template.send(TopicsConfig.RETRYABLE, key, Order.sample(key.hashCode() & 0xffff)).get(10, TimeUnit.SECONDS);
            }
            System.out.printf("%nsent to spring.retryable: %s; containers started: %s%n", retryKeys, retryIds);
            // ok x2 = 2, flaky2 = 3, flaky9 = 4 attempts + 1 DLT call = 10 entries
            await(() -> handler.retryable().size(), 10, Duration.ofSeconds(40), "retryable listener calls");
            support.stop(retryIds.toArray(String[]::new));

            var retries = new Table("t ms", "topic", "partition", "key", "attempt", "retry_topic-attempts header", "ms since original send", "listener");
            handler.retryable().stream().sorted(Comparator.comparingLong(ScriptedOrderHandler.Attempt::tMs)).forEach(a ->
                    retries.row(a.tMs(), (a.topic().equals(TopicsConfig.RETRYABLE) ? "main" : a.topic().substring(TopicsConfig.RETRYABLE.length())), a.partition(), a.key() + "  (" + FailureScript.describe(a.key()) + ")",
                            a.attempt(), a.springAttempt(), a.sinceSendMs(), a.outcome()));
            retries.print("2. @RetryableTopic(attempts=4, backOff=@BackOff(delay=1000, multiplier=2)): retry topics -retry-1000, -retry-2000, -retry-4000, then -dlt");

            // ---- 3. head-of-line blocking --------------------------------------------------------------------
            var hol = new Table("strategy", "slowest 'ok' record (ms from send to processing)", "why");
            hol.row("blocking (part 1)", maxOkDelay(handler.blocking()), "the partition waits while flaky9-6 is retried 3 times with back-off");
            hol.row("non-blocking (part 2)", maxOkDelay(handler.retryable()), "the failed record leaves the partition; ok records are processed at once");
            hol.print("3. what the innocent records paid");
        });
    }

    private static long maxOkDelay(List<ScriptedOrderHandler.Attempt> attempts) {
        return attempts.stream().filter(a -> a.key().startsWith("ok-")).mapToLong(ScriptedOrderHandler.Attempt::sinceSendMs).max().orElse(0);
    }

    /**
     * A plain consumer (String key, raw bytes) on the dead-letter topic, so the table shows exactly what was published.
     * It assigns the partitions instead of subscribing and commits nothing: a one-shot verification read has no use
     * for a consumer group, and this way it does not leave an empty one behind on every run.
     */
    private static List<ConsumerRecord<String, byte[]>> readDlt(int expected, Duration timeout) {
        var props = Env.consumer("spring-errors-dlt-reader", "dlt-reader");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        var records = new ArrayList<ConsumerRecord<String, byte[]>>();
        try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(TopicsConfig.ERRORS_DLT).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        // The topic was recreated at the start of this run, so a short read means the dead-letter path itself is
        // broken. Fail like every other wait in this module instead of printing a convincing but empty table.
        if (records.size() < expected) {
            throw new IllegalStateException("%s: expected %d dead-letter records within %s, got %d"
                    .formatted(TopicsConfig.ERRORS_DLT, expected, timeout, records.size()));
        }
        records.sort(Comparator.comparing(ConsumerRecord::key));
        return records;
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "-" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : new java.math.BigInteger(header.value()).intValue();
    }

    private static long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : new java.math.BigInteger(header.value()).longValue();
    }

    private static String simpleName(String fqcn) {
        return fqcn.substring(fqcn.lastIndexOf('.') + 1);
    }

    private static void await(IntSupplier value, int target, Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (value.getAsInt() < target) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(what + ": " + value.getAsInt() + " of " + target + " within " + timeout);
            }
            DemoSupport.sleep(50);
        }
    }
}
