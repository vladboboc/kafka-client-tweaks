package io.kafkatweaks.spring.share.recipe;

import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.share.ShareScript;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Chapter 20 · Share {@code @KafkaListener}s. A share listener looks like any other, except for the
 * {@code containerFactory} (a {@link org.springframework.kafka.config.ShareKafkaListenerContainerFactory} from
 * {@link ShareConfig}) and, in MANUAL mode, the {@link ShareAcknowledgment} parameter. {@code concurrency} is the number
 * of consumer threads (share consumers); unlike a consumer group it is not capped by the partition count. Two things
 * the annotation offers are ignored by the share container in 4.1: {@code clientIdPrefix} (the client.id is the
 * listener id, plus {@code -n} per consumer thread) and {@code properties} (client overrides belong on the factory).
 * <p>
 * {@code script.*} calls are the demo's scripted outcomes and measurement ({@code ShareScript}): your processing goes
 * there. Measured by the {@code spring-share} demo (docs/20-spring-share-consumers.md).
 */
@Component
@Profile("spring-share")
public class ShareListeners implements DisposableBean {

    private final ShareScript script;
    private final ExecutorService worker = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("slow-worker-", 0).factory());
    private final RenewWhileWorking renewing = new RenewWhileWorking(worker);

    public ShareListeners(ShareScript script) {
        this.script = script;
    }

    // ---- 1. EXPLICIT (the default): returning normally = ACCEPT, sent by the container ---------------------------------

    @KafkaListener(id = "share-explicit", groupId = "spring-share-explicit", topics = TopicsConfig.QUEUE,
            containerFactory = "shareKafkaListenerContainerFactory", concurrency = "4")
    public void explicit(ConsumerRecord<String, String> record) {
        script.work("share-explicit", record);
    }

    // ---- 2. MANUAL: the listener decides per record, and MUST decide -------------------------------------------------

    @KafkaListener(id = "share-manual", groupId = "spring-share-manual", topics = TopicsConfig.QUEUE,
            containerFactory = "manualShareContainerFactory", concurrency = "2")
    public void manual(ConsumerRecord<String, String> record, ShareAcknowledgment ack) {
        switch (script.manualDecision(record)) {
            case ACCEPT -> ack.acknowledge();   // done
            case RELEASE -> ack.release();      // "not now": back to the queue, deliveryCount + 1, any member may get it
            case REJECT -> ack.reject();        // poison: archived, never delivered again
            case NOTHING -> {
                // The bug this mode makes possible: no acknowledgement at all. The consumer thread that delivered this
                // record cannot poll again (the client refuses to poll with unacknowledged records), so its whole poll
                // is never committed. The demo does it once on purpose.
            }
        }
    }

    // ---- 3. EXPLICIT + a recoverer: exceptions become RELEASE or REJECT (ReleaseTransientRecoverer) --------------------

    @KafkaListener(id = "share-recover", groupId = "spring-share-recover", topics = TopicsConfig.QUEUE,
            containerFactory = "recoveringShareContainerFactory", concurrency = "2")
    public void recovering(ConsumerRecord<String, String> record) {
        script.workOrFail("share-recover", record);   // throws TransientFailure (-> RELEASE) or IllegalStateException (-> REJECT)
    }

    // ---- 4. acquisition locks: one slow record, three ways ---------------------------------------------------------------

    @KafkaListener(id = "share-lock-2s", groupId = "spring-share-lock-2s", topics = TopicsConfig.QUEUE_LOCKS,
            containerFactory = "shareKafkaListenerContainerFactory")
    public void lock2s(ConsumerRecord<String, String> record) {
        slowOnTheConsumerThread("share-lock-2s", record);
    }

    @KafkaListener(id = "share-lock-10s", groupId = "spring-share-lock-10s", topics = TopicsConfig.QUEUE_LOCKS,
            containerFactory = "shareKafkaListenerContainerFactory")
    public void lock10s(ConsumerRecord<String, String> record) {
        slowOnTheConsumerThread("share-lock-10s", record);
    }

    private void slowOnTheConsumerThread(String listenerId, ConsumerRecord<String, String> record) {
        script.track(listenerId, record);
        if (script.isSlow(record)) {
            script.slowWork();   // on the consumer thread: the poll's other records wait for their commit too
        }
    }

    /** MANUAL mode, the slow work on a worker, and {@code renew()} while it runs: see {@link RenewWhileWorking}. */
    @KafkaListener(id = "share-lock-renew", groupId = "spring-share-lock-renew", topics = TopicsConfig.QUEUE_LOCKS,
            containerFactory = "manualShareContainerFactory")
    public void lockRenew(ConsumerRecord<String, String> record, ShareAcknowledgment ack) {
        script.track("share-lock-renew", record);
        if (!script.isSlow(record)) {
            ack.acknowledge();
            return;
        }
        script.renewStep(renewing.onDelivery(record, ack, script::slowWork));
    }

    /** Also run when the context closes, so a demo that fails half way still leaves no worker behind. Idempotent. */
    @Override
    public void destroy() {
        shutdown();
    }

    public void shutdown() {
        worker.shutdown();
    }
}
