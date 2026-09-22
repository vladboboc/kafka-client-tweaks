package io.kafkatweaks.spring.errors;

import io.kafkatweaks.common.Order;
import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
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
 * The two listeners of chapter 18. Both follow {@link FailureScript}: the record key says whether the call
 * succeeds, throws a retryable {@link TransientFailure} for the first k attempts, or throws an
 * {@link IllegalArgumentException} the error handler is told never to retry. Poison pills never reach a listener.
 */
@Component
@Profile("spring-error-handling")
public class ErrorListeners {

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

    // ---- 1. blocking retries: DefaultErrorHandler with back-off, then a dead-letter topic ------------------------

    @KafkaListener(id = "errors-blocking", groupId = "spring-errors-blocking", clientIdPrefix = "errors-blocking",
            topics = TopicsConfig.ERRORS, containerFactory = "blockingRetryFactory")
    public void blocking(ConsumerRecord<String, Order> record) {
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

    @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2.0), include = TransientFailure.class,
            numPartitions = "3", autoCreateTopics = "true")
    @KafkaListener(id = "errors-retryable", groupId = "spring-errors-retryable", clientIdPrefix = "errors-retryable", topics = TopicsConfig.RETRYABLE)
    public void retryable(ConsumerRecord<String, Order> record) {
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

    @DltHandler
    public void retryableDlt(ConsumerRecord<String, Order> record) {
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
