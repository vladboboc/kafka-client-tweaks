package io.kafkatweaks.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Jackson-backed {@link Serializer} + {@link Deserializer} for one record type.
 * <p>
 * Kafka's client library has no {@code Serde} interface (that lives in Kafka Streams); a class that
 * implements both roles is the plain-client idiom. Instantiate it in code and hand the instance to the
 * {@code KafkaProducer}/{@code KafkaConsumer} constructor: that avoids the configure-by-class-name path,
 * which cannot carry the target type.
 */
public final class JsonSerde<T> implements Serializer<T>, Deserializer<T> {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new SerializationException("cannot serialize " + type.getSimpleName() + " for topic " + topic, e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return MAPPER.readValue(data, type);
        } catch (IOException e) {
            throw new SerializationException("cannot deserialize " + type.getSimpleName() + " from topic " + topic, e);
        }
    }

    /** Both interfaces declare default {@code configure}/{@code close}; Java requires the class to pick one. Nothing to do here. */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
