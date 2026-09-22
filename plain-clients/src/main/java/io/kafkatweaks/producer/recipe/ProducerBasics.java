package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Chapter 01 · A producer with nothing tuned, used the right way: named, asynchronous, checked.
 * <p>
 * The 4.x defaults are already safe ({@code acks=all}, idempotence on, {@code linger.ms=5}), so what matters in this
 * chapter is how you call {@code send()}, not a setting. Measured by the {@code producer-baseline} demo
 * (docs/01-producer-baseline.md): one {@code send().get()} costs a full round trip (~15 ms on the Docker stack), the
 * asynchronous send below pushed 5.9K records/s from one thread.
 * <pre>{@code
 * try (var producer = new KafkaProducer<String, String>(ProducerBasics.config("broker1:9092,broker2:9092", "order-service"))) {
 *     ProducerBasics.send(producer, new ProducerRecord<>("orders", orderId, json), e -> log.error("order {} lost", orderId, e));
 * }   // close() waits for the records still in flight
 * }</pre>
 */
public final class ProducerBasics {

    private ProducerBasics() {
    }

    /** All a producer needs. Everything else is a default, and the 4.x defaults are the safe ones. */
    public static Properties config(String bootstrapServers, String clientId) {
        var props = new Properties();
        // Any one reachable broker bootstraps the client; list several so a single broker being down does not matter.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Becomes the client-id tag on every producer metric and shows up in broker logs and quotas. Name it.
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * The normal way to send. Returns as soon as the record sits in the accumulator; the callback runs later on the
     * producer's I/O thread, once the broker acknowledged the batch or the producer gave up on it. Keep the callback
     * short and never block in it: it holds up every other callback of this producer.
     * <p>
     * Call {@code producer.flush()} when you must know that everything sent so far is acknowledged (before committing
     * consumer offsets, for example); {@code close()} also waits for the records still in flight.
     */
    public static <K, V> void send(Producer<K, V> producer, ProducerRecord<K, V> record, Consumer<Exception> onFailure) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                onFailure.accept(exception);   // the only place a failed send shows up: log it, count it, park the record
            }
        });
    }

    /**
     * Send one record and wait for its acknowledgement. Fine for a rare record that must be confirmed before the caller
     * goes on; in a loop it caps the producer at about {@code 1000 / round-trip-ms} records per second, because every
     * record waits for the previous one's full round trip and nothing is ever batched.
     */
    public static <K, V> RecordMetadata sendAndWait(Producer<K, V> producer, ProducerRecord<K, V> record)
            throws ExecutionException, InterruptedException {
        return producer.send(record).get();
    }
}
