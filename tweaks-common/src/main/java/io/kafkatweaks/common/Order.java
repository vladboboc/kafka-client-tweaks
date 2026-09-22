package io.kafkatweaks.common;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The domain event used by the consumer and serialization chapters. Small on purpose: the tutorial is
 * about the transport, not the domain.
 */
public record Order(String orderId, String customerId, BigDecimal totalAmount, String currency, Instant createdAt) {

    public static Order sample(long seq) {
        return new Order(
                "ORD-" + seq,
                Payloads.key(seq, 20),
                BigDecimal.valueOf(10 + (seq % 990), 2).add(BigDecimal.valueOf(seq % 100)),
                "EUR",
                Instant.now());
    }
}
