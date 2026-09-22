package io.kafkatweaks.spring.share.recipe;

import io.kafkatweaks.spring.errors.recipe.TransientFailure;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ShareConsumerRecordRecoverer;

/**
 * Chapter 20 · What happens to a record whose share listener threw. The default,
 * {@link ShareConsumerRecordRecoverer#REJECTING}, makes every failure final. This one retries what is worth retrying:
 * RELEASE (the record goes back to the queue, any member may get it, deliveryCount + 1, until
 * {@code share.delivery.count.limit}) for a {@link TransientFailure}, REJECT (archived, never delivered again) for
 * anything else. Measured: every record that threw a TransientFailure once came back and was processed.
 */
public final class ReleaseTransientRecoverer implements ShareConsumerRecordRecoverer {

    @Override
    public AcknowledgeType recover(ConsumerRecord<?, ?> record, Exception exception) {
        // The listener's exception arrives wrapped (ListenerExecutionFailedException): look down the cause chain.
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof TransientFailure) {
                return AcknowledgeType.RELEASE;
            }
        }
        return AcknowledgeType.REJECT;
    }
}
