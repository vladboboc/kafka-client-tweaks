package io.kafkatweaks.spring.serdes;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.kafkatweaks.avro.AvroTrust;
import io.kafkatweaks.common.Env;
import io.kafkatweaks.common.Order;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.serdes.recipe.SerdesConfig;
import io.kafkatweaks.spring.serdes.recipe.SerdesListeners;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Chapter 21: chapter 13's serialization questions, answered with Spring's serializers and properties. Measures
 * {@link SerdesConfig} and {@link SerdesListeners}; everything else in this file is measurement.
 * <ol>
 *   <li>JSON out with {@code JacksonJsonSerializer}: what the {@code __TypeId__} header carries</li>
 *   <li>JSON in, four ways: header + type mapping, ignore the header, remap the header, or convert in the listener adapter</li>
 *   <li>Avro out through a second producer factory: the untrusted-class failure first, then the registry subject</li>
 *   <li>Avro in with {@code specific.avro.reader}; wire size JSON vs Avro</li>
 * </ol>
 * <pre>
 *   records=1000
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-serdes")
public class SerdesDemo {

    private static final Logger log = LoggerFactory.getLogger(SerdesDemo.class);

    private record Raw(String headers, int valueBytes, String value) {
    }

    @Bean
    ApplicationRunner springSerdes(DemoSupport support, SerdesProbe probe, KafkaTemplate<String, Object> template, KafkaProperties properties) {
        return support.demo("spring-serdes", args -> {
            int records = args.getInt("records", 1000);
            String avroSubject = TopicsConfig.AVRO + "-value";
            try (var topics = new Topics()) {
                topics.recreate(TopicsConfig.SERDES, 3);
                topics.recreate(TopicsConfig.AVRO, 3);
                for (String group : List.of("spring-serdes-json", "spring-serdes-view", "spring-serdes-mapped", "spring-serdes-converter", "spring-serdes-avro")) {
                    topics.deleteGroup(group);
                }
            }
            String registryUrl = properties.getProperties().get("schema.registry.url");
            SchemaRegistryClient registry = new CachedSchemaRegistryClient(registryUrl, 100);
            resetSubject(registry, avroSubject);

            // ---- 1. JSON out ------------------------------------------------------------------------------------------
            long jsonValueBytes = 0;
            long jsonHeaderBytes = 0;
            List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                Order order = Order.sample(i);
                futures.add(template.send(TopicsConfig.SERDES, order.orderId(), order));
            }
            for (var future : futures) {
                SendResult<String, Object> result = future.get();
                jsonValueBytes += result.getRecordMetadata().serializedValueSize();
                for (Header h : result.getProducerRecord().headers()) {
                    jsonHeaderBytes += h.key().length() + h.value().length;
                }
            }
            Raw raw = peek(TopicsConfig.SERDES);
            log.info("1. {} io.kafkatweaks.common.Order records sent through the auto-configured KafkaTemplate (value-serializer: JacksonJsonSerializer)", records);
            log.info("the first record on the wire: headers [{}], value {} bytes:\n   {}", raw.headers(), raw.valueBytes(), raw.value());

            // ---- 2. JSON in, four ways -------------------------------------------------------------------------------------
            List<String> jsonListeners = List.of("serdes-json", "serdes-view", "serdes-mapped", "serdes-converter");
            support.start(jsonListeners.toArray(String[]::new));
            for (String id : jsonListeners) {
                await(() -> probe.received(id).count(), records, Duration.ofSeconds(60), id);
            }
            support.stop(jsonListeners.toArray(String[]::new));
            var in = new Table("listener", "how the value type is decided", "value class in the listener", "__TypeId__ header", "first value");
            row(in, probe, "serdes-json", "JacksonJsonDeserializer: header token -> spring.json.type.mapping (yml)");
            row(in, probe, "serdes-view", "spring.json.use.type.headers=false + value.default.type (per-listener properties)");
            row(in, probe, "serdes-mapped", "spring.json.type.mapping=order:OrderView (per-listener properties)");
            row(in, probe, "serdes-converter", "StringDeserializer + JacksonJsonMessageConverter: the method parameter's type");
            log.info("2. the same {} records read by four listeners\n{}", records, in);

            // ---- 3. Avro out: a second producer factory, failure first --------------------------------------------------
            var avroFactory = SerdesConfig.avroProducerFactory(properties, "spring-serdes-avro");   // <- the recipe under test
            var avroTemplate = new KafkaTemplate<>(avroFactory);
            log.info("3. Avro: KafkaTemplate over a DefaultKafkaProducerFactory built from spring.kafka.producer.* with value.serializer=KafkaAvroSerializer");
            try {
                avroTemplate.send(TopicsConfig.AVRO, "ORD-0", avroOrder(0)).get();
                log.warn("unexpected: the untrusted generated class serialized fine");
            } catch (Exception e) {
                Throwable root = rootCause(e);
                log.info("FAILED before AvroTrust: {}: {}", root.getClass().getSimpleName(), firstLine(root.getMessage()));
            }
            avroFactory.reset();   // drop the producer whose first send failed
            AvroTrust.trustGeneratedClasses();
            long avroValueBytes = 0;
            long avroHeaderBytes = 0;
            List<CompletableFuture<SendResult<String, Object>>> avroFutures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                avroFutures.add(avroTemplate.send(TopicsConfig.AVRO, "ORD-" + i, avroOrder(i)));
            }
            for (var future : avroFutures) {
                SendResult<String, Object> result = future.get();
                avroValueBytes += result.getRecordMetadata().serializedValueSize();
                for (Header h : result.getProducerRecord().headers()) {
                    avroHeaderBytes += h.key().length() + h.value().length;
                }
            }
            SchemaMetadata latest = registry.getLatestSchemaMetadata(avroSubject);
            log.info("after AvroTrust.trustGeneratedClasses(): {} records sent; registered subject {}, schema id {}, version {}",
                    records, avroSubject, latest.getId(), latest.getVersion());

            // ---- 4. Avro in ------------------------------------------------------------------------------------------------------
            support.start("serdes-avro");
            await(() -> probe.received("serdes-avro").count(), records, Duration.ofSeconds(60), "serdes-avro");
            support.stop("serdes-avro");
            var avroIn = new Table("listener", "how the value type is decided", "value class in the listener", "__TypeId__ header", "first value");
            row(avroIn, probe, "serdes-avro", "KafkaAvroDeserializer + specific.avro.reader=true: schema id -> registry -> generated class");
            log.info("4. avroContainerFactory: a DefaultKafkaConsumerFactory with the Confluent deserializer, spring.kafka.listener.* still applied by Boot's configurer\n{}", avroIn);

            var wire = new Table("format", "value bytes/record", "header bytes/record", "what is on the wire")
                    .row("JSON (JacksonJsonSerializer)", "%.1f".formatted((double) jsonValueBytes / records), "%.1f".formatted((double) jsonHeaderBytes / records),
                            "field names + values as text; __TypeId__ header with the mapped token")
                    .row("Confluent Avro (KafkaAvroSerializer)", "%.1f".formatted((double) avroValueBytes / records), "%.1f".formatted((double) avroHeaderBytes / records),
                            "magic byte 0x00 + 4-byte schema id + Avro binary, no field names, no headers");
            log.info("wire size, the same order data\n{}", wire);
            log.info("subjects in the registry for spring.* topics: {}",
                    registry.getAllSubjects().stream().filter(s -> s.startsWith("spring.")).sorted().collect(Collectors.joining(", ")));
            avroFactory.destroy();
        });
    }

    private static void row(Table table, SerdesProbe probe, String id, String how) {
        SerdesProbe.Received r = probe.received(id);
        table.row(id, how, r.valueClass(), r.typeIdHeader(), r.sample());
    }

    /** The generated (Avro) twin of {@code Order.sample(i)}: same fields, exact decimal scale 2 as the schema demands. */
    static io.kafkatweaks.avro.generated.Order avroOrder(long i) {
        Order sample = Order.sample(i);
        return io.kafkatweaks.avro.generated.Order.newBuilder()
                .setOrderId(sample.orderId())
                .setCustomerId(sample.customerId())
                .setTotalAmount(sample.totalAmount().setScale(2, RoundingMode.HALF_UP))
                .setCurrency(sample.currency())
                .setCreatedAt(sample.createdAt())
                .build();
    }

    /** Reads the first record of partition 0 as raw bytes: headers and value exactly as the broker stores them. */
    private static Raw peek(String topic) {
        // assign() + no commits: this reader never joins the group, so the group id is decorative and no empty
        // group is left behind. (It used to be suffixed with System.nanoTime() % 1_000_000, which is negative
        // whenever nanoTime() is - its origin is arbitrary - giving ids like "spring-serdes-peek--123456".)
        var props = Env.consumer("spring-serdes-peek", "serdes-peek");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(300))) {
                    String headers = "";
                    for (Header h : record.headers()) {
                        headers += (headers.isEmpty() ? "" : ", ") + h.key() + "=" + new String(h.value(), StandardCharsets.UTF_8);
                    }
                    return new Raw(headers, record.value().length, new String(record.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return new Raw("?", 0, "no record found in partition 0");
    }

    private static void resetSubject(SchemaRegistryClient registry, String subject) {
        try {
            registry.deleteSubject(subject);
            registry.deleteSubject(subject, true);
        } catch (Exception ignored) {
            // the subject did not exist
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

    private static void await(LongSupplier value, long target, Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (value.getAsLong() < target) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(what + ": reached " + value.getAsLong() + " of " + target + " within " + timeout);
            }
            DemoSupport.sleep(50);
        }
    }
}
