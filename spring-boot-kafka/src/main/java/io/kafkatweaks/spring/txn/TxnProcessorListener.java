package io.kafkatweaks.spring.txn;

import io.kafkatweaks.spring.TopicsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 19, part 4: chapter 06's consume-transform-produce processor as listeners. Nothing here says
 * "transaction": the container starts one (it got the {@code KafkaTransactionManager} from Boot), the template's
 * send joins it, and the input offsets are sent to the transaction before it commits.
 * <p>
 * The two listeners differ in one attribute. A <b>record</b> listener gets a transaction per record: 1 200 records
 * are 1 200 transactions. A <b>batch</b> listener gets a transaction per poll, which is what chapter 06 did by hand.
 * A crash rolls the current transaction back and its records come again.
 */
@Component
@Profile("spring-transactions")
public class TxnProcessorListener {

    private final KafkaTemplate<String, String> template;
    private final AtomicLong recordCalls = new AtomicLong();
    private final AtomicLong batchCalls = new AtomicLong();
    private final AtomicLong batchRecords = new AtomicLong();
    private final AtomicBoolean crashed = new AtomicBoolean();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private volatile long crashAtRecord = 800;

    public TxnProcessorListener(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    public void crashAtRecord(long n) {
        this.crashAtRecord = n;
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

    /** One transaction per record: begin, send, send offsets, commit. Safe, and expensive. */
    @KafkaListener(id = "txn-per-record", groupId = "spring-txn-record", clientIdPrefix = "txn-record", topics = TopicsConfig.TXN_IN)
    public void perRecord(ConsumerRecord<String, String> record) {
        recordCalls.incrementAndGet();
        template.send(TopicsConfig.TXN_OUT, record.key(), record.value().toUpperCase());   // joins the container's transaction
    }

    /** One transaction per poll (up to max.poll.records): the chapter-06 shape. Crashes once, in the middle of a batch. */
    @KafkaListener(id = "txn-per-batch", groupId = "spring-txn-batch", clientIdPrefix = "txn-batch", topics = TopicsConfig.TXN_IN, batch = "true")
    public void perBatch(List<ConsumerRecord<String, String>> records) {
        long call = batchCalls.incrementAndGet();
        int sent = 0;
        for (ConsumerRecord<String, String> record : records) {
            template.send(TopicsConfig.TXN_OUT, record.key(), record.value().toUpperCase());
            sent++;
            long n = batchRecords.incrementAndGet();
            if (n == crashAtRecord && crashed.compareAndSet(false, true)) {
                events.add("batch call %d: crash by script after %d of %d records were sent (input %s-%d@%d); the whole poll's transaction is rolled back and re-fetched"
                        .formatted(call, sent, records.size(), record.topic(), record.partition(), record.offset()));
                template.flush();   // demo only: the records sent so far reach the log, so read_uncommitted can show them as aborted
                throw new IllegalStateException("crash by script at record " + n);
            }
        }
    }
}
