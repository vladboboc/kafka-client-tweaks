package io.kafkatweaks.common;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSerdeTest {

    private final JsonSerde<Order> serde = new JsonSerde<>(Order.class);

    @Test
    void roundTripsAnOrderIncludingInstantAndBigDecimal() {
        Order order = new Order("ORD-1", "customer-7", new BigDecimal("149.90"), "EUR", Instant.parse("2026-09-21T10:15:30Z"));

        byte[] bytes = serde.serialize("t", order);
        Order back = serde.deserialize("t", bytes);

        assertThat(back).isEqualTo(order);
        assertThat(new String(bytes, StandardCharsets.UTF_8)).contains("\"createdAt\":\"2026-09-21T10:15:30Z\"");
    }

    @Test
    void nullsPassThrough() {
        assertThat(serde.serialize("t", null)).isNull();
        assertThat(serde.deserialize("t", null)).isNull();
    }

    @Test
    void ignoresUnknownFieldsSoOldConsumersSurviveNewProducers() {
        byte[] newer = "{\"orderId\":\"ORD-2\",\"customerId\":\"c\",\"totalAmount\":1,\"currency\":\"EUR\",\"createdAt\":\"2026-01-01T00:00:00Z\",\"channel\":\"web\"}"
                .getBytes(StandardCharsets.UTF_8);

        assertThat(serde.deserialize("t", newer).orderId()).isEqualTo("ORD-2");
    }

    @Test
    void wrapsMalformedInputInKafkaSerializationException() {
        assertThatThrownBy(() -> serde.deserialize("t", "not json".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class);
    }
}
