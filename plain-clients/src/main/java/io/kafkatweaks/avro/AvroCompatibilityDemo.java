package io.kafkatweaks.avro;

import io.kafkatweaks.Demo;
import io.kafkatweaks.avro.generated.Order;
import io.kafkatweaks.avro.recipe.AvroClients;
import io.kafkatweaks.avro.recipe.SchemaPipeline;
import io.kafkatweaks.common.Args;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Chapter 13: writer schema vs reader schema, BACKWARD vs FORWARD. Measures {@link SchemaPipeline} and the two reader
 * choices in {@link AvroClients} ({@code specificConsumer}: the compiled v1 class; {@code latestSchemaConsumer}: the
 * subject's latest version); everything else in this file is measurement.
 * <ol>
 *   <li>the reader x writer matrix for three changes, with Avro's own resolution rules (no registry)</li>
 *   <li>a subject at BACKWARD: v1, v2, v3 records, read by a v1-compiled consumer and by a latest-schema consumer</li>
 *   <li>the same with a subject at FORWARD</li>
 * </ol>
 * No arguments; Kafka client properties ({@code key.with.dots=value}) apply to the producer and consumers.
 */
public final class AvroCompatibilityDemo implements Demo {

    private static final Logger log = LoggerFactory.getLogger(AvroCompatibilityDemo.class);
    private static final String BACKWARD_TOPIC = "tweaks.avro-compat-backward";
    private static final String FORWARD_TOPIC = "tweaks.avro-compat-forward";
    private static final Conversions.DecimalConversion DECIMAL = new Conversions.DecimalConversion();
    private static final Set<String> SHOWN_FIELDS = Set.of("orderId", "customerId", "channel", "salesRep");
    private static final Pattern MISSING_FIELD = Pattern.compile("missing required field \\w+");

    /** One schema change, applied to the previous version. */
    record Change(String label, UnaryOperator<Schema> apply) {
    }

    private static final Change CHANNEL = new Change("add channel (default \"web\")", OrderSchemas::plusChannel);
    private static final Change SALES_REP = new Change("add salesRep (no default)", OrderSchemas::plusSalesRep);
    private static final Change NO_CUSTOMER = new Change("drop customerId (no default)", OrderSchemas::minusCustomerId);

    @Override
    public void run(Args args) throws Exception {
        // The v1 reader below is the generated Order class: Avro >= 1.12.1 must be told to trust it.
        AvroTrust.trustGeneratedClasses();
        SchemaRegistryClient registry = new CachedSchemaRegistryClient(Env.schemaRegistryUrl(), 100);
        try (var topics = new Topics()) {
            topics.recreate(BACKWARD_TOPIC, 1);
            topics.recreate(FORWARD_TOPIC, 1);
        }
        Registry.resetSubjects(registry, BACKWARD_TOPIC + "-value", FORWARD_TOPIC + "-value");

        matrix();
        scenario(args, registry, 2, "BACKWARD", BACKWARD_TOPIC, NO_CUSTOMER, SALES_REP);
        scenario(args, registry, 3, "FORWARD", FORWARD_TOPIC, SALES_REP, NO_CUSTOMER);
        summary();
    }

    // ------------------------------------------------------------------ 1. the matrix

    private static void matrix() {
        log.info("""
                1. writer schema vs reader schema
                   writer schema  the schema the producer serialized with; its id is in bytes 1-4 of every record
                   reader schema  the schema the consumer decodes into: the compiled class (specific.avro.reader=true)
                                  or the subject's latest version (use.latest.version=true)
                   Avro resolves the two: writer fields the reader does not know are skipped, reader fields the writer
                   did not have are filled from their default (no default = the record cannot be read).
                   BACKWARD = the NEW schema can read OLD data.  FORWARD = the OLD schema can read NEW data.""");
        Schema v1 = OrderSchemas.v1();
        var table = new Table("change to v1", "new reader, old data (BACKWARD)", "old reader, new data (FORWARD)", "satisfies");
        for (Change c : List.of(CHANNEL, SALES_REP, NO_CUSTOMER)) {
            Schema next = c.apply().apply(v1);
            var backward = SchemaPipeline.canRead(next, v1);   // <- the recipe under test
            var forward = SchemaPipeline.canRead(v1, next);
            table.row(c.label(), verdict(backward), verdict(forward), level(compatible(backward), compatible(forward)));
        }
        log.info("SchemaPipeline.canRead(reader, writer) for each direction, Avro only, no registry\n{}", table);
    }

    // ------------------------------------------------------------------ 2./3. one subject, three versions, two readers

    private static void scenario(Args args, SchemaRegistryClient registry, int step, String level, String topic,
                                 Change accepted, Change rejected) throws Exception {
        String subject = topic + "-value";
        log.info("{}. subject {} at {}: {}", step, subject, level, level.equals("BACKWARD")
                ? "every new version must read the previous version's data (upgrade consumers first)"
                : "the previous version must read every new version's data (upgrade producers first)");
        SchemaPipeline.pinCompatibility(registry, subject, level);   // <- the recipe under test

        Schema v1 = OrderSchemas.v1();
        Schema v2 = CHANNEL.apply().apply(v1);
        Schema v3 = accepted.apply().apply(v2);
        var writers = new HashMap<Integer, String>();   // schema id -> what the demo calls it
        var ci = new Table("candidate", "register under " + level);
        register(registry, subject, v1, "v1 (the compiled Order)", ci, writers);
        register(registry, subject, v2, "v2 = v1, " + CHANNEL.label(), ci, writers);
        register(registry, subject, rejected.apply().apply(v2), "v2, " + rejected.label(), ci, writers);
        register(registry, subject, v3, "v3 = v2, " + accepted.label(), ci, writers);
        log.info("the CI pipeline registers the candidates (SchemaPipeline.register)\n{}", ci);

        produce(args, topic, List.of(AvroDemo.order(1), generic(AvroDemo.order(2), v2), generic(AvroDemo.order(3), v3)));
        log.info("produced 3 records to {}, one per writer schema, with auto.register.schemas=false", topic);

        read(args, topic, writers, "v1: compiled Order class", AvroClients.specificConsumer(Env.schemaRegistryUrl()));
        read(args, topic, writers, "latest registered: v3", AvroClients.latestSchemaConsumer(Env.schemaRegistryUrl()));
    }

    private static void register(SchemaRegistryClient registry, String subject, Schema schema, String label,
                                 Table ci, Map<Integer, String> writers) throws Exception {
        try {
            int id = SchemaPipeline.register(registry, subject, schema);   // <- the recipe under test
            int version = registry.getVersion(subject, new AvroSchema(schema));
            writers.put(id, "v" + version);
            ci.row(label, "OK, id " + id + ", version " + version);
        } catch (RestClientException e) {
            ci.row(label, "REJECTED " + Registry.rejection(e));
        }
    }

    /** One producer stands in for three generations of the producing service: v1 class, then v2 and v3 records. */
    private static void produce(Args args, String topic, List<Object> values) throws Exception {
        Properties p = Env.producer("avro-compat-producer");
        p.putAll(AvroClients.producer(Env.schemaRegistryUrl()));
        // Only schemas the pipeline registered can be written: the serializer looks the id up, it never registers.
        p.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        args.applyOverrides(p);
        try (var producer = new KafkaProducer<String, Object>(p)) {
            for (Object value : values) {
                String key = value instanceof Order o ? o.getOrderId() : String.valueOf(((GenericRecord) value).get("orderId"));
                producer.send(new ProducerRecord<>(topic, key, value)).get();
            }
        }
    }

    /**
     * Reads every record of the topic with a KafkaAvroDeserializer configured from {@code recipe}. A KafkaConsumer
     * calls exactly that per record; calling it here lets the table show each record's writer schema id and carry on
     * past a record that cannot be read (a KafkaConsumer throws RecordDeserializationException from poll() there).
     */
    private static void read(Args args, String topic, Map<Integer, String> writers, String reader, Map<String, Object> recipe) {
        var table = new Table("offset", "writer schema (id in the record)", "result");
        Properties props = Env.consumer("avro-compat-" + System.nanoTime(), "avro-compat-reader");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        args.applyOverrides(props);
        try (var consumer = new KafkaConsumer<String, byte[]>(props);
             var deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(recipe, false);   // <- the recipe under test
            consumer.subscribe(List.of(topic));
            var records = new ArrayList<ConsumerRecord<String, byte[]>>();
            int idle = 0;
            while (records.size() < 3 && idle < 15) {
                var batch = consumer.poll(Duration.ofMillis(300));
                idle = batch.isEmpty() ? idle + 1 : 0;
                batch.forEach(records::add);
            }
            for (var r : records) {
                int id = ByteBuffer.wrap(r.value()).getInt(1);   // byte 0 is the magic byte, 1-4 the schema id
                String writer = "id " + id + " = " + writers.getOrDefault(id, "?");
                try {
                    table.row(r.offset(), writer, "OK  " + describe(deserializer.deserialize(topic, r.value())));
                } catch (SerializationException e) {
                    table.row(r.offset(), writer, "FAILED  " + failure(e));
                }
            }
        }
        log.info("reader schema {}\n{}", reader, table);
    }

    // ------------------------------------------------------------------ 4. summary

    private static void summary() {
        var levels = new Table("level", "registry checks", "upgrade first", "a v1-compiled consumer reading newer records")
                .row("BACKWARD (default)", "new schema reads old data", "consumers", "not guaranteed: broke on v3 (drop customerId)")
                .row("FORWARD", "old schema reads new data", "producers", "guaranteed: read v1, v2, v3")
                .row("FULL", "both", "either", "guaranteed; only fields WITH defaults may come and go")
                .row("*_TRANSITIVE", "the same against ALL versions", "", "needed when consumers lag several versions");
        log.info("""
                what each level guarantees
                {}
                "add channel" (with a default) passed every check: adding (or removing) a field WITH a default is the change
                  that keeps both old and new readers working. FULL makes the registry enforce exactly that.""", levels);
    }

    // ------------------------------------------------------------------ helpers

    /** {@code order} as a GenericRecord of {@code schema}, with raw logical-type values, as an untyped producer sends it. */
    private static GenericRecord generic(Order order, Schema schema) {
        var record = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            Object value = switch (f.name()) {
                case "channel" -> "mobile";
                case "salesRep" -> "SR-7";
                default -> order.get(f.name());
            };
            if (value instanceof BigDecimal amount) {
                value = DECIMAL.toBytes(amount, f.schema(), f.schema().getLogicalType());
            } else if (value instanceof Instant at) {
                value = at.toEpochMilli();
            }
            record.put(f.name(), value);
        }
        return record;
    }

    private static String describe(Object value) {
        if (value instanceof Order o) {
            return "Order(orderId=" + o.getOrderId() + ", customerId=" + o.getCustomerId() + ")";
        }
        var g = (GenericRecord) value;
        return "GenericRecord(" + g.getSchema().getFields().stream()
                .map(Schema.Field::name)
                .filter(SHOWN_FIELDS::contains)
                .map(n -> n + "=" + g.get(n))
                .collect(Collectors.joining(", ")) + ")";
    }

    private static String failure(SerializationException e) {
        String message = Registry.rootCause(e).getMessage();
        var m = MISSING_FIELD.matcher(message == null ? "" : message);
        return m.find() ? m.group() : Registry.firstLine(message);
    }

    private static boolean compatible(SchemaCompatibility.SchemaPairCompatibility p) {
        return p.getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
    }

    private static String verdict(SchemaCompatibility.SchemaPairCompatibility p) {
        if (compatible(p)) {
            return "yes";
        }
        var first = p.getResult().getIncompatibilities().getFirst();
        return "no: " + first.getType() + " (" + first.getMessage() + ")";
    }

    private static String level(boolean backward, boolean forward) {
        return backward && forward ? "FULL" : backward ? "BACKWARD only" : forward ? "FORWARD only" : "NONE";
    }
}
