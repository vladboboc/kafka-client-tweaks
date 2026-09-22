package io.kafkatweaks.spring.errors.recipe;

import io.kafkatweaks.common.Order;
import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

/**
 * Chapter 18 · The same listener twice, retried two ways.
 * <ul>
 *   <li><b>Blocking</b>: the container's {@code DefaultErrorHandler} ({@link ErrorHandlingRecipe#blockingRetryFactory})
 *   seeks back and redelivers after each back-off. The partition waits: an innocent record behind a failing one took
 *   3.4 s in the demo.</li>
 *   <li><b>Non-blocking</b>: {@code @RetryableTopic} re-publishes the failed record to {@code -retry-1000},
 *   {@code -retry-2000}, {@code -retry-4000} and finally {@code -dlt}, each with its own container, while the partition
 *   moves on (the innocent record: 527 ms). The price: ordering per key is gone, the record is copied per attempt.</li>
 * </ul>
 */
@Component
@Profile("spring-error-handling")
public class OrderListeners {

    private final OrderHandler orders;

    public OrderListeners(OrderHandler orders) {
        this.orders = orders;
    }

    @KafkaListener(id = "errors-blocking", groupId = "spring-errors-blocking", clientIdPrefix = "errors-blocking",
            topics = TopicsConfig.ERRORS, containerFactory = "blockingRetryFactory")
    public void blocking(ConsumerRecord<String, Order> record) {
        orders.handle(record);
    }

    // attempts = 1 delivery + 3 retries; numPartitions: the retry topics default to ONE partition, set it
    @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2.0), include = TransientFailure.class,
            numPartitions = "3", autoCreateTopics = "true")
    @KafkaListener(id = "errors-retryable", groupId = "spring-errors-retryable", clientIdPrefix = "errors-retryable", topics = TopicsConfig.RETRYABLE)
    public void retryable(ConsumerRecord<String, Order> record) {
        orders.handle(record);
    }

    /** What exhausted its attempts arrives here; the exception travels in the {@code kafka_exception-*} headers. */
    @DltHandler
    public void parked(ConsumerRecord<String, Order> record) {
        orders.parked(record);
    }
}
