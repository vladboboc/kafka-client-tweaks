package io.kafkatweaks.spring.template.recipe;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 15 · A {@link ProducerListener} bean replaces Boot's {@code LoggingProducerListener} and is called by the
 * auto-configured {@code KafkaTemplate} for every acknowledgement: the place for send metrics, audit
 * counters or alerting on failures, without touching the call sites.
 */
public final class CountingProducerListener implements ProducerListener<Object, Object> {

    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        successes.incrementAndGet();
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata, Exception exception) {
        failures.incrementAndGet();
    }

    public long successes() {
        return successes.get();
    }

    public long failures() {
        return failures.get();
    }
}
