package io.kafkatweaks.spring.serdes;

import io.kafkatweaks.common.Order;
import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Five listeners reading the same JSON records (or their Avro twins) with five different deserialization set-ups.
 * Each remembers how many records it saw, the class of the first value, the {@code __TypeId__} header it came with
 * and a sample, so the demo can put them side by side.
 */
@Component
@Profile("spring-serdes")
public class SerdesListeners {

    public static final class Received {
        private final AtomicLong count = new AtomicLong();
        private volatile String valueClass = "-";
        private volatile String typeIdHeader = "-";
        private volatile String sample = "-";

        public long count() {
            return count.get();
        }

        public String valueClass() {
            return valueClass;
        }

        public String typeIdHeader() {
            return typeIdHeader;
        }

        public String sample() {
            return sample;
        }
    }

    private final Map<String, Received> received = new ConcurrentHashMap<>();

    public Received received(String listenerId) {
        return received.computeIfAbsent(listenerId, k -> new Received());
    }

    private void note(String listenerId, Object value, Iterable<org.apache.kafka.common.header.Header> headers) {
        Received r = received(listenerId);
        if (r.count.incrementAndGet() == 1) {
            r.valueClass = value == null ? "null" : value.getClass().getName();
            r.sample = value == null ? "null" : abbreviate(value.toString(), 70);
            r.typeIdHeader = "(none)";
            for (org.apache.kafka.common.header.Header h : headers) {
                if (h.key().equals(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)) {
                    r.typeIdHeader = new String(h.value(), StandardCharsets.UTF_8);
                }
            }
        }
    }

    static String abbreviate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }

    // ---- 1. the Spring default: JacksonJsonDeserializer, type from the __TypeId__ header ----------------------------

    /** The header says {@code order}; the consumer's type mapping (application-spring-serdes.yml) says that is io.kafkatweaks.common.Order. */
    @KafkaListener(id = "serdes-json", groupId = "spring-serdes-json", topics = TopicsConfig.SERDES)
    public void json(ConsumerRecord<String, Order> record) {
        note("serdes-json", record.value(), record.headers());
    }

    // ---- 2. the consumer decides the type ------------------------------------------------------------------------------

    /** Ignore the header, always deserialize into OrderView: per-listener overrides of the deserializer's properties. */
    @KafkaListener(id = "serdes-view", groupId = "spring-serdes-view", topics = TopicsConfig.SERDES,
            properties = {"spring.json.use.type.headers:false", "spring.json.value.default.type:io.kafkatweaks.spring.serdes.OrderView"})
    public void view(ConsumerRecord<String, OrderView> record) {
        note("serdes-view", record.value(), record.headers());
    }

    /** Keep using the header, but map its token {@code order} to a class of this service's choosing. */
    @KafkaListener(id = "serdes-mapped", groupId = "spring-serdes-mapped", topics = TopicsConfig.SERDES,
            properties = "spring.json.type.mapping:order:io.kafkatweaks.spring.serdes.OrderView")
    public void mapped(ConsumerRecord<String, Object> record) {
        note("serdes-mapped", record.value(), record.headers());
    }

    // ---- 3. the other wiring: StringDeserializer + a message converter, the method parameter picks the type ------------

    @KafkaListener(id = "serdes-converter", groupId = "spring-serdes-converter", topics = TopicsConfig.SERDES,
            containerFactory = "converterContainerFactory")
    public void converted(Order order, @Header(KafkaHeaders.RECEIVED_KEY) String key,
                          @Header(name = AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, required = false) String typeId) {
        Received r = received("serdes-converter");
        if (r.count.incrementAndGet() == 1) {
            r.valueClass = order.getClass().getName();
            r.sample = abbreviate(order.toString(), 70);
            r.typeIdHeader = typeId == null ? "(none)" : typeId;
        }
    }

    // ---- 4. Confluent Avro through a second consumer factory --------------------------------------------------------------

    @KafkaListener(id = "serdes-avro", groupId = "spring-serdes-avro", topics = TopicsConfig.AVRO,
            containerFactory = "avroContainerFactory")
    public void avro(ConsumerRecord<String, io.kafkatweaks.avro.generated.Order> record) {
        note("serdes-avro", record.value(), record.headers());
    }
}
