package io.kafkatweaks.spring.serdes.recipe;

import io.kafkatweaks.common.Order;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.serdes.OrderView;
import io.kafkatweaks.spring.serdes.SerdesProbe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Chapter 21 · Five listeners reading the same JSON records (or their Avro twins) with five different deserialization
 * set-ups: who decides the value's type, the producer's {@code __TypeId__} header or the consumer. The defaults come
 * from application-spring-serdes.yml; everything per listener is an attribute here. Measured by the
 * {@code spring-serdes} demo (docs/21-spring-serialization.md): the same record arrived as
 * {@code io.kafkatweaks.common.Order}, as this service's own {@code OrderView}, and as a generated Avro class.
 * {@code probe.note(...)} is the demo's measurement: your processing goes there.
 */
@Component
@Profile("spring-serdes")
public class SerdesListeners {

    private final SerdesProbe probe;

    public SerdesListeners(SerdesProbe probe) {
        this.probe = probe;
    }

    // ---- 1. the Spring default: JacksonJsonDeserializer, type from the __TypeId__ header ----------------------------

    /** The header says {@code order}; the consumer's type mapping (application-spring-serdes.yml) says that is io.kafkatweaks.common.Order. */
    @KafkaListener(id = "serdes-json", groupId = "spring-serdes-json", topics = TopicsConfig.SERDES)
    public void json(ConsumerRecord<String, Order> record) {
        probe.note("serdes-json", record.value(), record.headers());
    }

    // ---- 2. the consumer decides the type ------------------------------------------------------------------------------

    /** Ignore the header, always deserialize into OrderView: per-listener overrides of the deserializer's properties. */
    @KafkaListener(id = "serdes-view", groupId = "spring-serdes-view", topics = TopicsConfig.SERDES,
            properties = {"spring.json.use.type.headers:false", "spring.json.value.default.type:io.kafkatweaks.spring.serdes.OrderView"})
    public void view(ConsumerRecord<String, OrderView> record) {
        probe.note("serdes-view", record.value(), record.headers());
    }

    /** Keep using the header, but map its token {@code order} to a class of this service's choosing. */
    @KafkaListener(id = "serdes-mapped", groupId = "spring-serdes-mapped", topics = TopicsConfig.SERDES,
            properties = "spring.json.type.mapping:order:io.kafkatweaks.spring.serdes.OrderView")
    public void mapped(ConsumerRecord<String, Object> record) {
        probe.note("serdes-mapped", record.value(), record.headers());
    }

    // ---- 3. the other wiring: StringDeserializer + a message converter, the method parameter picks the type ------------

    @KafkaListener(id = "serdes-converter", groupId = "spring-serdes-converter", topics = TopicsConfig.SERDES,
            containerFactory = "converterContainerFactory")
    public void converted(Order order, @Header(KafkaHeaders.RECEIVED_KEY) String key,
                          @Header(name = AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, required = false) String typeId) {
        probe.note("serdes-converter", order, typeId);
    }

    // ---- 4. Confluent Avro through a second consumer factory --------------------------------------------------------------

    @KafkaListener(id = "serdes-avro", groupId = "spring-serdes-avro", topics = TopicsConfig.AVRO,
            containerFactory = "avroContainerFactory")
    public void avro(ConsumerRecord<String, io.kafkatweaks.avro.generated.Order> record) {
        probe.note("serdes-avro", record.value(), record.headers());
    }
}
