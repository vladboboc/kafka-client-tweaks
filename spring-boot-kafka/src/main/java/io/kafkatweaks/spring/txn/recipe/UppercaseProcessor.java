package io.kafkatweaks.spring.txn.recipe;

import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.txn.TxnProbe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Chapter 19 · Chapter 06's exactly-once consume-transform-produce processor as listeners. Nothing here says
 * "transaction": the container starts one (it got the {@code KafkaTransactionManager} from Boot, because
 * {@code transaction-id-prefix} is set), the template's send joins it, and the input offsets are sent to the
 * transaction before it commits.
 * <p>
 * The two listeners differ in one attribute. A <b>record</b> listener gets a transaction per record: 1 200 records are
 * 1 200 transactions. A <b>batch</b> listener gets a transaction per poll, which is what chapter 06 did by hand. A crash
 * rolls the current transaction back and its records come again. Measured by the {@code spring-transactions} demo
 * (docs/19-spring-transactions.md): the batch listener crashed once mid-batch, and the committed output still held every
 * input record exactly once. {@code probe.*} calls are the demo's measurement (and its scripted crash).
 */
@Component
@Profile("spring-transactions")
public class UppercaseProcessor {

    private final KafkaTemplate<String, String> template;
    private final TxnProbe probe;

    public UppercaseProcessor(KafkaTemplate<String, String> template, TxnProbe probe) {
        this.template = template;
        this.probe = probe;
    }

    /** One transaction per record: begin, send, send offsets, commit. Safe, and expensive. */
    @KafkaListener(id = "txn-per-record", groupId = "spring-txn-record", clientIdPrefix = "txn-record", topics = TopicsConfig.TXN_IN)
    public void perRecord(ConsumerRecord<String, String> record) {
        probe.recordCall();
        template.send(TopicsConfig.TXN_OUT, record.key(), record.value().toUpperCase());   // joins the container's transaction
    }

    /** One transaction per poll (up to max.poll.records): the chapter-06 shape. */
    @KafkaListener(id = "txn-per-batch", groupId = "spring-txn-batch", clientIdPrefix = "txn-batch", topics = TopicsConfig.TXN_IN, batch = "true")
    public void perBatch(List<ConsumerRecord<String, String>> records) {
        probe.batchCall();
        for (ConsumerRecord<String, String> record : records) {
            template.send(TopicsConfig.TXN_OUT, record.key(), record.value().toUpperCase());
            probe.sent(records, record);   // the demo's crash: throws once, in the middle of a batch
        }
    }
}
