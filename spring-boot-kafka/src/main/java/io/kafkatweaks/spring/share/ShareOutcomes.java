package io.kafkatweaks.spring.share;

import io.kafkatweaks.spring.errors.TransientFailure;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.ShareConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two things the share container does not report on its own.
 * <ul>
 *   <li>What the <b>broker</b> answered to the acknowledgements the container sent. The container commits them and
 *   ignores the result; the {@link AcknowledgementCommitCallback} it registers on every share consumer (a container
 *   property) sees every completed commit, successful or refused (e.g. {@code InvalidRecordStateException} when the
 *   acquisition lock had already expired).</li>
 *   <li>What the {@link ShareConsumerRecordRecoverer} decided for records whose listener threw: RELEASE (try again,
 *   on any member, delivery count + 1) for a {@link TransientFailure}, REJECT (archive, never again) for anything else.
 *   The default recoverer is {@link ShareConsumerRecordRecoverer#REJECTING}: every failure is final.</li>
 * </ul>
 */
@Component
@Profile("spring-share")
public class ShareOutcomes implements AcknowledgementCommitCallback, ShareConsumerRecordRecoverer {

    private final Map<String, AtomicLong> committed = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> refused = new ConcurrentHashMap<>();
    private final AtomicLong released = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private volatile String lastRefusal = "";

    @Override
    public void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
        offsets.forEach((tip, set) ->
                (exception == null ? committed : refused).computeIfAbsent(tip.topic(), k -> new AtomicLong()).addAndGet(set.size()));
        if (exception != null) {
            lastRefusal = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        }
    }

    @Override
    public AcknowledgeType recover(ConsumerRecord<?, ?> record, Exception exception) {
        // The listener's exception arrives wrapped (ListenerExecutionFailedException): look down the cause chain.
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof TransientFailure) {
                released.incrementAndGet();
                return AcknowledgeType.RELEASE;
            }
        }
        rejected.incrementAndGet();
        return AcknowledgeType.REJECT;
    }

    /** Acknowledgements of records of this topic the broker accepted (ACCEPT, RELEASE, REJECT and RENEW alike). */
    public long committed(String topic) {
        return committed.getOrDefault(topic, new AtomicLong()).get();
    }

    /** Acknowledgements the broker refused, typically because the record's acquisition lock had expired. */
    public long refused(String topic) {
        return refused.getOrDefault(topic, new AtomicLong()).get();
    }

    public String lastRefusal() {
        return lastRefusal;
    }

    public long released() {
        return released.get();
    }

    public long rejected() {
        return rejected.get();
    }
}
