package io.kafkatweaks.spring.serdes;

import java.math.BigDecimal;

/**
 * What a downstream service might keep of an {@code io.kafkatweaks.common.Order}: its own class, two of the five
 * fields. It does not (and must not) know the producer's class; chapter 21 shows two ways to deserialize into it
 * regardless of what the {@code __TypeId__} header says.
 */
public record OrderView(String orderId, BigDecimal totalAmount) {
}
