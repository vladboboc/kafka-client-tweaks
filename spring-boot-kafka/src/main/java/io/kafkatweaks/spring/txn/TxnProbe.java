package io.kafkatweaks.spring.txn;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 19's measurement inside the recipe listeners ({@code recipe/UppercaseProcessor}): calls and records per
 * listener, and the scripted crash of the batch listener at record {@code crashAtRecord}, exactly once.
 */
@Component
@Profile("spring-transactions")
public class TxnProbe {

    private final KafkaTemplate<String, String> template;
    private final AtomicLong recordCalls = new AtomicLong();
    private final AtomicLong batchCalls = new AtomicLong();
    private final AtomicLong batchRecords = new AtomicLong();
    private final AtomicBoolean crashed = new AtomicBoolean();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private volatile long crashAtRecord = 800;

    public TxnProbe(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    public void crashAtRecord(long n) {
        this.crashAtRecord = n;
    }

    public void recordCall() {
        recordCalls.incrementAndGet();
    }

    public void batchCall() {
        batchCalls.incrementAndGet();
    }

    /** One record of a batch was sent. At record {@code crashAtRecord}, the first time only: flush and throw. */
    public void sent(List<ConsumerRecord<String, String>> batch, ConsumerRecord<String, String> record) {
        long n = batchRecords.incrementAndGet();
        if (n == crashAtRecord && crashed.compareAndSet(false, true)) {
            events.add("batch call %d: crash by script after %d of %d records were sent (input %s-%d@%d); the whole poll's transaction is rolled back and re-fetched"
                    .formatted(batchCalls.get(), batch.indexOf(record) + 1, batch.size(), record.topic(), record.partition(), record.offset()));
            template.flush();   // demo only: the records sent so far reach the log, so read_uncommitted can show them as aborted
            throw new IllegalStateException("crash by script at record " + n);
        }
    }

    public long recordCalls() {
        return recordCalls.get();
    }

    public long batchCalls() {
        return batchCalls.get();
    }

    public long batchRecords() {
        return batchRecords.get();
    }

    public List<String> events() {
        return List.copyOf(events);
    }
}
