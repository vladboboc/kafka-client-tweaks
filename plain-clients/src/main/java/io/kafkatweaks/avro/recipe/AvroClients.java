package io.kafkatweaks.avro.recipe;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Chapter 13 · Avro with the Schema Registry: under a third of the bytes of JSON, and schema changes checked before a
 * consumer ever sees them.
 * <p>
 * The serializer registers (or looks up) the schema under a subject ({@code <topic>-value} by default), caches the id
 * and prefixes every record with it; the deserializer fetches the writer schema by id, once. Measured by the
 * {@code avro-roundtrip} demo (docs/13-avro-schema-registry.md), the same order data:
 * <pre>
 *   Confluent Avro     37.39 bytes/record   magic byte + 4-byte schema id + Avro binary, no field names
 *   JSON (Jackson)       129 bytes/record
 * </pre>
 * The generated classes must be trusted once per JVM before the first send or read (Avro >= 1.12.1, on BOTH sides):
 * {@code AvroTrust.trustGeneratedClasses()} at startup, see {@link io.kafkatweaks.avro.AvroTrust}.
 */
public final class AvroClients {

    private AvroClients() {
    }

    /** Values as Avro. Registers the object's schema if the subject does not know it yet: fine for development. */
    public static Map<String, Object> producer(String schemaRegistryUrl) {
        return Map.of(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    /**
     * The production pair: schemas are registered by a pipeline, and the application can only produce what is
     * already agreed. With an empty subject the send fails ("Subject ... not found"), which is the point.
     */
    public static Map<String, Object> productionProducer(String schemaRegistryUrl) {
        var config = new HashMap<>(producer(schemaRegistryUrl));
        // Default true: any refactor of the class silently becomes a new schema version, registered by whichever
        // service happens to deploy first.
        config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        // Serialize with the subject's latest registered version instead of the class's own schema. With the default
        // latest.compatibility.strict=true the serializer refuses if the class is not compatible with that version.
        config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        return config;
    }

    /** Several event types in one topic: the subject is the record's full name instead of {@code <topic>-value}. */
    public static Map<String, Object> recordNameSubjects() {
        return Map.of(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
    }

    /** Values back into the generated classes ({@code specific.avro.reader}, default false = {@code GenericRecord}). */
    public static Map<String, Object> specificConsumer(String schemaRegistryUrl) {
        return Map.of(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    /**
     * {@code GenericRecord}s read with the subject's LATEST schema as reader schema: old records arrive in the new shape,
     * with added fields filled from their defaults. That is what BACKWARD compatibility buys: deploy consumers first.
     */
    public static Map<String, Object> latestSchemaConsumer(String schemaRegistryUrl) {
        return Map.of(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
    }
}
