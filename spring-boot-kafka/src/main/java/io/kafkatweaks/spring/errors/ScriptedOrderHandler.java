package io.kafkatweaks.spring.errors;

import io.kafkatweaks.common.Order;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.errors.recipe.OrderHandler;
import io.kafkatweaks.spring.errors.recipe.TransientFailure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Chapter 18's stand-in for a real service behind the recipe listeners ({@code recipe/OrderListeners}). It follows
 * {@link FailureScript}: the record key says whether the call succeeds, throws a retryable {@link TransientFailure} for
 * the first k attempts, or throws an {@link IllegalArgumentException} the error handler is told never to retry. Poison
 * pills never reach it. Every attempt is recorded for the demo's tables.
 */
@Component
@Profile("spring-error-handling")
public class ScriptedOrderHandler implements OrderHandler {

    public record Attempt(long tMs, String topic, int partition, String key, int attempt, String springAttempt, long sinceSendMs, String outcome) {
    }

    private final List<Attempt> blocking = new CopyOnWriteArrayList<>();
    private final List<Attempt> retryable = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private volatile long startedAt = System.currentTimeMillis();

    /** Called by the demo right before it produces: records older than this belong to an earlier run and are ignored. */
    public void markStart() {
        startedAt = System.currentTimeMillis();
    }

    public List<Attempt> blocking() {
        return List.copyOf(blocking);
    }

    public List<Attempt> retryable() {
        return List.copyOf(retryable);
    }

    @Override
    public void handle(ConsumerRecord<String, Order> record) {
        if (record.topic().equals(TopicsConfig.ERRORS)) {
            blocking(record);
        } else {
            retryable(record);   // spring.retryable and its -retry-* topics
        }
    }

    // ---- 1. blocking retries: DefaultErrorHandler with back-off, then a dead-letter topic ------------------------

    private void blocking(ConsumerRecord<String, Order> record) {
        if (record.timestamp() < startedAt) {
            return;   // leftover from an earlier run
        }
        int attempt = attempts.computeIfAbsent("blocking:" + record.key(), k -> new AtomicInteger()).incrementAndGet();
        FailureScript.Plan plan = FailureScript.parse(record.key());
        String outcome = switch (plan.kind()) {
            case FATAL -> "throws IllegalArgumentException";
            case FLAKY -> plan.failsOn(attempt) ? "throws TransientFailure" : "processed";
            default -> "processed";
        };
        blocking.add(new Attempt(System.currentTimeMillis() - startedAt, record.topic(), record.partition(), record.key(), attempt,
                headerInt(record, KafkaHeaders.DELIVERY_ATTEMPT), System.currentTimeMillis() - record.timestamp(), outcome));
        if (plan.kind() == FailureScript.Kind.FATAL) {
            throw new IllegalArgumentException("fatal by script: " + record.key());
        }
        if (plan.failsOn(attempt)) {
            throw new TransientFailure(record.key() + " attempt " + attempt);
        }
    }

    // ---- 2. non-blocking retries: the failed record is re-published to a retry topic, the partition moves on ---------

    private void retryable(ConsumerRecord<String, Order> record) {
        long originalTimestamp = headerLong(record, RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, record.timestamp());
        if (originalTimestamp < startedAt) {
            return;   // leftover from an earlier run (retry topics are not recreated between runs)
        }
        int attempt = attempts.computeIfAbsent("retryable:" + record.key(), k -> new AtomicInteger()).incrementAndGet();
        FailureScript.Plan plan = FailureScript.parse(record.key());
        String outcome = plan.failsOn(attempt) ? "throws TransientFailure" : "processed";
        retryable.add(new Attempt(System.currentTimeMillis() - startedAt, record.topic(), record.partition(), record.key(), attempt,
                headerInt(record, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS), System.currentTimeMillis() - originalTimestamp, outcome));
        if (plan.failsOn(attempt)) {
            throw new TransientFailure(record.key() + " attempt " + attempt);
        }
    }

    @Override
    public void parked(ConsumerRecord<String, Order> record) {
        long originalTimestamp = headerLong(record, RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, record.timestamp());
        if (originalTimestamp < startedAt) {
            return;
        }
        // Retry topics carry the exception as kafka_exception-*; a plain DeadLetterPublishingRecoverer uses kafka_dlt-exception-*.
        Header exception = record.headers().lastHeader(KafkaHeaders.EXCEPTION_FQCN);
        if (exception == null) {
            exception = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN);
        }
        String fqcn = exception == null ? "?" : new String(exception.value(), StandardCharsets.UTF_8);
        int attempt = attempts.getOrDefault("retryable:" + record.key(), new AtomicInteger()).get();
        retryable.add(new Attempt(System.currentTimeMillis() - startedAt, record.topic(), record.partition(), record.key(), attempt,
                headerInt(record, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS), System.currentTimeMillis() - originalTimestamp,
                "@DltHandler: " + fqcn.substring(fqcn.lastIndexOf('.') + 1)));
    }

    /** spring-kafka writes its numeric headers as big-endian two's-complement bytes (4 for int, 8 for long, or minimal). */
    private static String headerInt(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "-" : String.valueOf(new BigInteger(header.value()).intValue());
    }

    private static long headerLong(ConsumerRecord<?, ?> record, String name, long fallback) {
        Header header = record.headers().lastHeader(name);
        return header == null ? fallback : new BigInteger(header.value()).longValue();
    }
}
