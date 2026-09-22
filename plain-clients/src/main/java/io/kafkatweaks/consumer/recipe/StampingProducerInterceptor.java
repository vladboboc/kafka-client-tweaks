package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Chapter 12 · A {@link ProducerInterceptor}: code that runs inside the producer on every send and acknowledgement.
 * This one stamps a {@code sent-at} header (read by {@link LatencyConsumerInterceptor}) and counts acknowledgements.
 * Tracing (OpenTelemetry's Kafka instrumentation is one of these), audit counters and policy checks live here.
 * <p>
 * Register by class name, so it needs a public no-arg constructor:
 * {@code props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, StampingProducerInterceptor.class.getName())}.
 * It runs on the sending thread ({@code onSend}) and the I/O thread ({@code onAcknowledgement}): keep both fast.
 */
public final class StampingProducerInterceptor implements ProducerInterceptor<String, String> {

    /** Static only because the demo reads them; a real interceptor would publish metrics instead. */
    public static final AtomicInteger SENT = new AtomicInteger();
    public static final AtomicInteger ACKED = new AtomicInteger();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        record.headers().add("sent-at", Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        SENT.incrementAndGet();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        ACKED.incrementAndGet();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
