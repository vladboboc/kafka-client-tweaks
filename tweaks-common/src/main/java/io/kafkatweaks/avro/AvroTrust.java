package io.kafkatweaks.avro;

import io.kafkatweaks.avro.generated.Order;
import org.apache.avro.util.ClassSecurityValidator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers the generated Avro classes with Avro's class allow-list.
 * <p>
 * Since Avro 1.12.1 (CVE-2024-47561 hardening) Avro refuses to resolve <em>any</em> class that is not
 * explicitly trusted. With Confluent 8.3.x serializers that check fires on both sides: the
 * {@code KafkaAvroSerializer} resolves the record's class to its schema through {@code SpecificData}, and the
 * {@code KafkaAvroDeserializer} with {@code specific.avro.reader=true} resolves the writer schema's full name
 * ({@code io.kafkatweaks.avro.generated.Order}) to a class. Either fails with
 * {@code SecurityException: Forbidden ... This class is not trusted} until the class is on the list.
 * Avro 1.12.2 replaced the read-once system properties ({@code -Dorg.apache.avro.SERIALIZABLE_PACKAGES=...})
 * with this live, code-configurable validator.
 * <p>
 * {@code GenericRecord} consumers never instantiate generated classes and do not need this.
 */
public final class AvroTrust {

    private static final AtomicBoolean APPLIED = new AtomicBoolean();

    private AvroTrust() {
    }

    /** Idempotent. Extends the existing global predicate, so whatever Avro trusts by default stays trusted. */
    public static void trustGeneratedClasses() {
        if (!APPLIED.compareAndSet(false, true)) {
            return;
        }
        var previous = ClassSecurityValidator.getGlobal();
        var generated = ClassSecurityValidator.builder().add(Order.class).build();
        ClassSecurityValidator.setGlobal(clazz -> previous.isTrusted(clazz) || generated.isTrusted(clazz));
    }
}
