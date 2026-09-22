package io.kafkatweaks.spring.share;

import io.kafkatweaks.spring.errors.recipe.TransientFailure;
import io.kafkatweaks.spring.share.recipe.ReleaseTransientRecoverer;
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
 * Chapter 20's measurement around the two hooks {@code recipe/ShareConfig} plugs into its container factories (the
 * only bean of each type): two things the share container does not report on its own.
 * <ul>
 *   <li>What the <b>broker</b> answered to the acknowledgements the container sent. The container commits them and
 *   ignores the result; the {@link AcknowledgementCommitCallback} it registers on every share consumer (a container
 *   property) sees every completed commit, successful or refused (e.g. {@code InvalidRecordStateException} when the
 *   acquisition lock had already expired).</li>
 *   <li>What the {@link ShareConsumerRecordRecoverer} decided for records whose listener threw. The decision itself is
 *   {@link ReleaseTransientRecoverer}'s: RELEASE (try again, on any member, delivery count + 1) for a
 *   {@link TransientFailure}, REJECT (archive, never again) for anything else; this class counts it.</li>
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
    private final ShareConsumerRecordRecoverer recoverer = new ReleaseTransientRecoverer();

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
        AcknowledgeType decision = recoverer.recover(record, exception);
        (decision == AcknowledgeType.RELEASE ? released : rejected).incrementAndGet();
        return decision;
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
