package io.kafkatweaks.avro.recipe;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;

import java.io.IOException;

/**
 * Chapter 13 · Writer schema vs reader schema: what the CI pipeline runs before a producer ships a new schema.
 * <p>
 * Every record carries the id of its WRITER schema; the consumer decodes it into its READER schema (the compiled class,
 * or the subject's latest version), and Avro resolves the two: writer fields the reader does not know are skipped, reader
 * fields the writer did not have need a default. The subject's compatibility level decides which direction the registry
 * guarantees. Measured by the {@code avro-compatibility} demo (docs/13-avro-schema-registry.md):
 * <pre>
 *   change to v1                  new reader, old data (BACKWARD)   old reader, new data (FORWARD)
 *   +channel  (with default)      yes                               yes      -> FULL
 *   +salesRep (no default)        no                                yes      -> FORWARD only
 *   -customerId (no default)      yes                               no       -> BACKWARD only
 * </pre>
 * A consumer compiled against v1 survives v2 records only when v2 is FORWARD (or FULL) compatible with v1.
 */
public final class SchemaPipeline {

    private SchemaPipeline() {
    }

    /** The knob: the subject's compatibility level, checked by every later {@link #register}. */
    public static void pinCompatibility(SchemaRegistryClient registry, String subject, String level)
            throws IOException, RestClientException {
        // BACKWARD (registry default): the new schema can read data written with the previous one. Upgrade consumers first.
        // FORWARD: the previous schema can read data written with the new one. Upgrade producers first.
        // FULL: both. Add _TRANSITIVE to check against every registered version instead of only the latest.
        registry.updateCompatibility(subject, level);
    }

    /** Registers the next version; the registry answers HTTP 409 when it breaks the subject's level. */
    public static int register(SchemaRegistryClient registry, String subject, Schema schema)
            throws IOException, RestClientException {
        // Producers run auto.register.schemas=false, so this pipeline step is the only way a version gets in.
        return registry.register(subject, new AvroSchema(schema));
    }

    /** Can {@code reader} decode data written with {@code writer}? Avro's own rules: a unit test, no registry needed. */
    public static SchemaCompatibility.SchemaPairCompatibility canRead(Schema reader, Schema writer) {
        // BACKWARD asks canRead(newSchema, oldSchema); FORWARD asks canRead(oldSchema, newSchema).
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    }
}
