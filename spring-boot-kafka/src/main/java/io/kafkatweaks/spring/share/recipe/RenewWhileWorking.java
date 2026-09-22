package io.kafkatweaks.spring.share.recipe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.ShareAcknowledgment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Chapter 20 · A share record that takes longer than its acquisition lock: work on another thread, and {@code renew()}
 * the lock from the listener until the work is done.
 * <p>
 * A renewal is an acknowledgement type: the client sends it with the next poll, the broker extends the lock, and the
 * record comes back from that poll (same delivery, same {@code deliveryCount}), so the MANUAL listener sees it again
 * about once per container poll while the worker is busy. Every decision (renew again, or acknowledge because the work
 * is done) is taken there, on the consumer thread, with the acknowledgment of the current delivery. The worker never
 * touches an acknowledgment: between a renewal being sent and the record coming back it is not in flight, and an
 * acknowledgement queued from another thread in that window fails. Measured: a 3 s record under a 2 s lock, renewed
 * until done and acknowledged once, with no refused acknowledgement.
 */
public final class RenewWhileWorking {

    public enum Step {
        /** First delivery: the work was handed to the worker and the lock renewed. */
        STARTED,
        /** The record came back while the worker is still busy: renewed again. */
        STILL_WORKING,
        /** The work is done: acknowledged (ACCEPT). */
        DONE
    }

    private final ExecutorService worker;
    private final Map<String, Future<?>> jobs = new ConcurrentHashMap<>();

    public RenewWhileWorking(ExecutorService worker) {
        this.worker = worker;
    }

    /** Call from a MANUAL share listener for every delivery of a slow record. */
    public Step onDelivery(ConsumerRecord<?, ?> record, ShareAcknowledgment ack, Runnable work) {
        String id = record.topic() + "-" + record.partition() + "@" + record.offset();
        Future<?> job = jobs.get(id);
        if (job == null) {
            jobs.put(id, worker.submit(work));
            ack.renew();
            return Step.STARTED;
        }
        if (job.isDone()) {
            jobs.remove(id);
            ack.acknowledge();
            return Step.DONE;
        }
        ack.renew();
        return Step.STILL_WORKING;
    }
}
