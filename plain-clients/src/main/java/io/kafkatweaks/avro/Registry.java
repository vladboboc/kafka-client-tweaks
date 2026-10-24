package io.kafkatweaks.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.util.regex.Pattern;

/** Schema Registry and error-message helpers shared by the chapter 13 demos. Measurement only, no tweak in here. */
final class Registry {

    private static final Pattern ERROR_TYPE = Pattern.compile("errorType:'([A-Z_]+)'");

    private Registry() {
    }

    /** Registers {@code schema} and describes the outcome: {@code OK, id .., version ..} or {@code REJECTED ...}. */
    static String tryRegister(SchemaRegistryClient registry, String subject, Schema schema) {
        try {
            int id = registry.register(subject, new AvroSchema(schema));
            return "OK, id " + id + ", version " + registry.getLatestSchemaMetadata(subject).getVersion();
        } catch (RestClientException e) {
            return "REJECTED " + rejection(e);
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
    }

    /** {@code <HTTP status>/<error code>: <registry error type>}. */
    static String rejection(RestClientException e) {
        // The message embeds the whole previous schema; keep the status and the registry's error type.
        var m = ERROR_TYPE.matcher(e.getMessage());
        String why = m.find() ? m.group(1) : firstLine(e.getMessage());
        return e.getStatus() + "/" + e.getErrorCode() + ": " + why;
    }

    static String compatibility(SchemaRegistryClient registry, String subject) throws Exception {
        try {
            return registry.getCompatibility(subject);
        } catch (RestClientException e) {
            return "BACKWARD (inherited from the global default)";
        }
    }

    /** Soft- then hard-deletes each subject, and its compatibility override, so a rerun starts from version 1. */
    static void resetSubjects(SchemaRegistryClient registry, String... subjects) {
        for (String s : subjects) {
            try {
                registry.deleteSubject(s);
                registry.deleteSubject(s, true);
            } catch (Exception ignored) {
                // subject did not exist
            }
            try {
                registry.deleteCompatibility(s);
            } catch (Exception ignored) {
                // no subject-level override
            }
        }
    }

    static Throwable rootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
