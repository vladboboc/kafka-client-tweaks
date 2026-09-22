package io.kafkatweaks.spring.errors.recipe;

import io.kafkatweaks.common.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Chapter 18 · What {@link OrderListeners} do with an order: your service. Throwing is how a listener says "failed":
 * a {@link TransientFailure} is retried, an {@code IllegalArgumentException} is not (see {@link ErrorHandlingRecipe}).
 * In the demo this is {@code ScriptedOrderHandler}, which fails on cue and records every attempt.
 */
public interface OrderHandler {

    void handle(ConsumerRecord<String, Order> record);

    /** A record that exhausted its retries on the non-blocking path (the {@code @DltHandler}). */
    void parked(ConsumerRecord<String, Order> record);
}
