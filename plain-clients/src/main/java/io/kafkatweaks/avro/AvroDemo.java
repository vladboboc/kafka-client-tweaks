package io.kafkatweaks.avro;

import io.kafkatweaks.Demo;
import io.kafkatweaks.avro.generated.Order;
import io.kafkatweaks.avro.recipe.AvroClients;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.JsonSerde;
import io.kafkatweaks.common.Payloads;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Chapter 13: Confluent Avro serialization with the Schema Registry. Measures {@link AvroClients} and {@link AvroTrust};
 * everything else in this file is measurement.
 * <ol>
 *   <li>round trip with generated {@code SpecificRecord} classes; wire size vs JSON; what the first send costs</li>
 *   <li>the serializer knobs: auto.register.schemas, subject name strategies, use.latest.version</li>
 *   <li>schema evolution against the registry's compatibility check</li>
 * </ol>
 * <pre>
 *   records=1000
 * </pre>
 */
public final class AvroDemo implements Demo {

    private static final Logger log = LoggerFactory.getLogger(AvroDemo.class);
    private static final String TOPIC = "tweaks.avro";
    private static final String SUBJECT = TOPIC + "-value";

    @Override
    public void run(Args args) throws Exception {
        int records = args.getInt("records", 1000);
        SchemaRegistryClient registry = new CachedSchemaRegistryClient(Env.schemaRegistryUrl(), 100);

        try (var topics = new Topics()) {
            topics.recreate(TOPIC, 3);
            topics.delete(TOPIC + "-noreg");
        }
        resetSubjects(registry, SUBJECT, Order.getClassSchema().getFullName(), TOPIC + "-noreg-value");
        log.info("schema registry: {}   subject for {} values: {}", Env.schemaRegistryUrl(), TOPIC, SUBJECT);

        roundTrip(args, registry, records);
        knobs(args, registry);
        evolution(args, registry);
    }

    // ------------------------------------------------------------------ 1. round trip

    private static void roundTrip(Args args, SchemaRegistryClient registry, int records) throws Exception {
        log.info("1. producing generated Order records with KafkaAvroSerializer (auto.register.schemas=true, the default)");

        // First attempt, before trusting the generated class: Avro >= 1.12.1 refuses to resolve it, on the
        // SERIALIZER side too (the Confluent serializer looks the schema up through SpecificData by class).
        try (var producer = new KafkaProducer<String, Order>(avroProducerProps(args, "avro-untrusted"))) {
            producer.send(new ProducerRecord<>(TOPIC, "ORD-0", order(0))).get();
            log.warn("unexpected: the untrusted class serialized fine");
        } catch (Exception e) {
            Throwable root = rootCause(e);
            log.info("FAILED before AvroTrust: {}: {}", root.getClass().getSimpleName(), firstLine(root.getMessage()));
        }
        AvroTrust.trustGeneratedClasses();
        log.info("after AvroTrust.trustGeneratedClasses():");

        long avroBytes = 0;
        double firstSendMs, laterAvgMs;
        try (var producer = new KafkaProducer<String, Order>(avroProducerProps(args, "avro-producer"))) {
            long t0 = System.nanoTime();
            RecordMetadata first = producer.send(new ProducerRecord<>(TOPIC, "ORD-0", order(0))).get();
            firstSendMs = (System.nanoTime() - t0) / 1e6;
            avroBytes += first.serializedValueSize();
            t0 = System.nanoTime();
            for (int i = 1; i < records; i++) {
                avroBytes += producer.send(new ProducerRecord<>(TOPIC, "ORD-" + i, order(i))).get().serializedValueSize();
            }
            laterAvgMs = (System.nanoTime() - t0) / 1e6 / (records - 1);
        }
        SchemaMetadata latest = registry.getLatestSchemaMetadata(SUBJECT);
        log.info("registered: subject {}, schema id {}, version {}", SUBJECT, latest.getId(), latest.getVersion());
        log.info("first send: {} ms (schema registration + lookup over HTTP), later sends: {} ms avg (schema id cached in the serializer)",
                Math.round(firstSendMs), "%.2f".formatted(laterAvgMs));

        long jsonBytes = 0;
        try (var json = new JsonSerde<>(io.kafkatweaks.common.Order.class)) {
            for (int i = 0; i < records; i++) {
                jsonBytes += json.serialize(TOPIC, io.kafkatweaks.common.Order.sample(i)).length;
            }
        }
        var sizes = new Table("format", "bytes/record", "what is on the wire")
                .row("Confluent Avro", (double) avroBytes / records, "magic byte 0x00 + 4-byte schema id + Avro binary (no field names)")
                .row("JSON (Jackson)", (double) jsonBytes / records, "field names + values as text, no schema id");
        log.info("payload size, same order data\n{}", sizes);

        log.info("consuming with KafkaAvroDeserializer, specific.avro.reader=true:");
        Order sample = consumeSpecific(args, records);
        log.info("read {} records; first: {}", records, sample);
        log.info("""
                the deserializer reads the schema id, fetches the writer schema from the registry once (cached), and
                  resolves the schema's full name to the generated class. Avro >= 1.12.1 refuses class lookups (on both
                  sides) unless the class is trusted, hence AvroTrust (Avro's live ClassSecurityValidator, which replaced the
                  read-once org.apache.avro.SERIALIZABLE_PACKAGES system property in 1.12.2).""");
    }

    private static Order consumeSpecific(Args args, int expected) {
        Properties props = Env.consumer("avro-" + System.nanoTime(), "avro-consumer");
        props.putAll(AvroClients.specificConsumer(Env.schemaRegistryUrl()));   // <- the recipe under test
        args.applyOverrides(props);
        Order first = null;
        int n = 0;
        try (var consumer = new KafkaConsumer<String, Order>(props)) {
            consumer.subscribe(List.of(TOPIC));
            int idle = 0;
            while (n < expected && idle < 10) {
                var batch = consumer.poll(Duration.ofMillis(300));
                if (batch.isEmpty()) {
                    idle++;
                    continue;
                }
                for (ConsumerRecord<String, Order> r : batch) {
                    if (first == null) {
                        first = r.value();
                    }
                    n++;
                }
            }
        }
        return first;
    }

    // ------------------------------------------------------------------ 2. knobs

    private static void knobs(Args args, SchemaRegistryClient registry) throws Exception {
        log.info("""
                2. serializer knobs
                   auto.register.schemas   true: the serializer registers whatever it is given (fine for dev, dangerous in prod:
                                           a refactor silently becomes a new schema version). false: the schema must already exist.
                   use.latest.version      serialize with the latest registered version of the subject instead of the object's own schema
                   latest.compatibility.strict  when using the latest version, refuse if the object's schema is not compatible with it
                   value.subject.name.strategy  TopicNameStrategy (<topic>-value, one schema type per topic, default),
                                           RecordNameStrategy (<record full name>, many event types in one topic),
                                           TopicRecordNameStrategy (<topic>-<record full name>)
                   normalize.schemas       ignore irrelevant ordering when comparing/registering schemas""");
        var table = new Table("setting", "outcome");

        Properties noReg = avroProducerProps(args, "avro-noreg");
        noReg.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        try (var producer = new KafkaProducer<String, Order>(noReg)) {
            try (var topics = new Topics()) {
                topics.ensure(TOPIC + "-noreg", 1);
            }
            producer.send(new ProducerRecord<>(TOPIC + "-noreg", "k", order(1))).get();
            table.row("auto.register.schemas=false, new subject", "unexpected: succeeded");
        } catch (ExecutionException | org.apache.kafka.common.errors.SerializationException e) {
            table.row("auto.register.schemas=false, subject has no schema yet", "send fails: " + firstLine(rootCause(e).getMessage()));
        }

        Properties recordName = avroProducerProps(args, "avro-recordname");
        recordName.putAll(AvroClients.recordNameSubjects());
        try (var producer = new KafkaProducer<String, Order>(recordName)) {
            producer.send(new ProducerRecord<>(TOPIC, "k", order(2))).get();
        }
        table.row("value.subject.name.strategy=RecordNameStrategy", "registered under subject '" + Order.getClassSchema().getFullName() + "'");

        Properties latest = avroProducerProps(args, "avro-latest");
        latest.putAll(AvroClients.productionProducer(Env.schemaRegistryUrl()));   // <- the recipe under test
        try (var producer = new KafkaProducer<String, Order>(latest)) {
            producer.send(new ProducerRecord<>(TOPIC, "k", order(3))).get();
        }
        table.row("auto.register.schemas=false + use.latest.version=true", "send OK: uses the registry's latest version of " + SUBJECT + " (the production setting)");
        log.info("what each setting did\n{}", table);
        log.info("subjects now in the registry: {}", registry.getAllSubjects());
    }

    // ------------------------------------------------------------------ 3. evolution

    private static void evolution(Args args, SchemaRegistryClient registry) throws Exception {
        log.info("3. evolution. Subject compatibility is {} (registry default BACKWARD: a NEW schema must be able to read data written with the OLD one).",
                compatibility(registry, SUBJECT));
        Schema v1 = Order.getClassSchema();
        Schema v2 = withField(v1, "{\"name\":\"channel\",\"type\":\"string\",\"default\":\"web\"}");
        Schema v3 = withField(v1, "{\"name\":\"salesRep\",\"type\":\"string\"}");
        Schema v4 = withFieldType(v1, "currency", Schema.create(Schema.Type.INT));

        var table = new Table("candidate schema", "compatible with v1 under BACKWARD?", "register");
        table.row("v2 = v1 + channel:string default \"web\"", registry.testCompatibility(SUBJECT, new AvroSchema(v2)), tryRegister(registry, SUBJECT, v2));
        table.row("v3 = v1 + salesRep:string (NO default)", registry.testCompatibility(SUBJECT, new AvroSchema(v3)), tryRegister(registry, SUBJECT, v3));
        table.row("v4 = v1 with currency: string -> int", registry.testCompatibility(SUBJECT, new AvroSchema(v4)), tryRegister(registry, SUBJECT, v4));
        log.info("three candidate schemas for {}\n{}", SUBJECT, table);

        log.info("reading the v1 records with the LATEST schema (v2) as reader schema, as a GenericRecord:");
        Properties props = Env.consumer("avro-generic-" + System.nanoTime(), "avro-generic");
        props.putAll(AvroClients.latestSchemaConsumer(Env.schemaRegistryUrl()));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        args.applyOverrides(props);
        try (var consumer = new KafkaConsumer<String, GenericRecord>(props)) {
            consumer.subscribe(List.of(TOPIC));
            GenericRecord r = null;
            int idle = 0;
            while (r == null && idle < 15) {
                var batch = consumer.poll(Duration.ofMillis(300));
                if (batch.isEmpty()) {
                    idle++;
                } else {
                    r = batch.iterator().next().value();
                }
            }
            if (r != null) {
                log.info("reader schema fields: {}", r.getSchema().getFields().stream().map(Schema.Field::name).toList());
                log.info("record: {}", r);
                log.info("'channel' is filled from the v2 default: old data, new shape. That is what BACKWARD compatibility buys.");
            }
        }

        log.info("forcing the incompatible v3 in by setting the subject to NONE (what you do NOT want in production):");
        registry.updateCompatibility(SUBJECT, "NONE");
        log.info("compatibility now {}; register v3 -> {}", compatibility(registry, SUBJECT), tryRegister(registry, SUBJECT, v3));
        registry.updateCompatibility(SUBJECT, "BACKWARD");
        log.info("restored to {}. A consumer using v3 as reader schema can no longer read v1/v2 records (salesRep has no default).", compatibility(registry, SUBJECT));

        var levels = new Table("level", "who must be upgraded first", "allowed changes (Avro)")
                .row("BACKWARD (default)", "consumers", "add field WITH default, remove field")
                .row("FORWARD", "producers", "add field, remove field WITH default")
                .row("FULL", "either", "add/remove fields WITH defaults only")
                .row("*_TRANSITIVE", "same, checked against ALL versions, not just the latest", "")
                .row("NONE", "nobody checks anything", "everything; expect broken consumers");
        log.info("compatibility levels\n{}", levels);
    }

    // ------------------------------------------------------------------ helpers

    private static Order order(long i) {
        return Order.newBuilder()
                .setOrderId("ORD-" + i)
                .setCustomerId(Payloads.key(i, 20))
                .setTotalAmount(BigDecimal.valueOf(10 + (i % 990), 2).add(BigDecimal.valueOf(i % 100)))
                .setCurrency("EUR")
                .setCreatedAt(Instant.now())
                .build();
    }

    private static Properties avroProducerProps(Args args, String clientId) {
        Properties p = Env.producer(clientId);
        p.putAll(AvroClients.producer(Env.schemaRegistryUrl()));
        return args.applyOverrides(p);
    }

    private static Schema withField(Schema base, String fieldJson) {
        String json = base.toString();
        int end = json.lastIndexOf("]}");
        return new Schema.Parser().parse(json.substring(0, end) + "," + fieldJson + json.substring(end));
    }

    /** Copy of {@code base} with one field's type replaced (and its default dropped, since it would no longer fit). */
    private static Schema withFieldType(Schema base, String fieldName, Schema newType) {
        var fields = new java.util.ArrayList<Schema.Field>();
        for (Schema.Field f : base.getFields()) {
            fields.add(f.name().equals(fieldName)
                    ? new Schema.Field(f.name(), newType, f.doc())
                    : new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
        }
        return Schema.createRecord(base.getName(), base.getDoc(), base.getNamespace(), false, fields);
    }

    private static String tryRegister(SchemaRegistryClient registry, String subject, Schema schema) {
        try {
            int id = registry.register(subject, new AvroSchema(schema));
            return "OK, id " + id + ", version " + registry.getLatestSchemaMetadata(subject).getVersion();
        } catch (RestClientException e) {
            // The message embeds the whole previous schema; keep the status and the registry's error type.
            var m = java.util.regex.Pattern.compile("errorType:'([A-Z_]+)'").matcher(e.getMessage());
            String why = m.find() ? m.group(1) : firstLine(e.getMessage());
            return "REJECTED " + e.getStatus() + "/" + e.getErrorCode() + ": " + why;
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
    }

    private static String compatibility(SchemaRegistryClient registry, String subject) throws Exception {
        try {
            return registry.getCompatibility(subject);
        } catch (RestClientException e) {
            return "BACKWARD (inherited from the global default)";
        }
    }

    private static void resetSubjects(SchemaRegistryClient registry, String... subjects) {
        for (String s : subjects) {
            try {
                registry.deleteSubject(s);
                registry.deleteSubject(s, true);
            } catch (Exception ignored) {
                // subject did not exist
            }
        }
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
