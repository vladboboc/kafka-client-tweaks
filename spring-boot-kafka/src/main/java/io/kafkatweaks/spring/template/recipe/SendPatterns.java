package io.kafkatweaks.spring.template.recipe;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Chapter 15 · How to call a {@code KafkaTemplate}. {@code send()} is always asynchronous and returns a
 * {@code CompletableFuture<SendResult>}; waiting is the caller's choice, and the expensive one. Measured by the
 * {@code spring-template} demo (docs/15-spring-kafkatemplate.md), the same 1 000 records:
 * <pre>
 *   sendAndWait() per record      114 records/s     a round trip per record, nothing batches
 *   sendAllAndWait()           15 520 records/s     one wait for all of them
 * </pre>
 */
public final class SendPatterns {

    private SendPatterns() {
    }

    /** One record, confirmed before the caller goes on. Fine for a rare record; in a loop it serialises the round trips. */
    public static <K, V> SendResult<K, V> sendAndWait(KafkaTemplate<K, V> template, String topic, K key, V value)
            throws InterruptedException, ExecutionException, TimeoutException {
        return template.send(topic, key, value).get(10, TimeUnit.SECONDS);
    }

    /**
     * Hand every record to the producer, then wait once: the batches form while the loop runs. A failed record fails
     * the join; attach {@code whenComplete(...)} to the individual futures for per-record handling.
     */
    public static <K, V> List<SendResult<K, V>> sendAllAndWait(KafkaTemplate<K, V> template, List<ProducerRecord<K, V>> records) {
        List<CompletableFuture<SendResult<K, V>>> futures = records.stream().map(template::send).toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    /**
     * Spring's messaging API: {@code KafkaHeaders.TOPIC} and {@code KafkaHeaders.KEY} are consumed by the template, every
     * other header becomes a Kafka record header.
     */
    public static <K, V> CompletableFuture<SendResult<K, V>> sendWithHeaders(KafkaTemplate<K, V> template, String topic, K key, V value,
                                                                            Map<String, Object> headers) {
        var message = MessageBuilder.withPayload(value)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key);
        headers.forEach(message::setHeader);
        return template.send(message.build());
    }
}
