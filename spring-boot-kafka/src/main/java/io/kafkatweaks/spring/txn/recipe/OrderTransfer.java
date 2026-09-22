package io.kafkatweaks.spring.txn.recipe;

import io.kafkatweaks.spring.TopicsConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Chapter 19 · Spring's declarative transactions on top of Kafka. The auto-configured {@code KafkaTransactionManager}
 * is a {@code PlatformTransactionManager}, so {@code @Transactional} begins a Kafka transaction before the method and
 * commits it after; every {@code KafkaTemplate} operation inside joins it, and an exception rolls it back (the records
 * are in the log, marked aborted, and read_committed consumers never see them). Measured: transfer t1 committed its 3
 * records, transfer t2 (threw) committed 0.
 */
@Service
@Profile("spring-transactions")
public class OrderTransfer {

    private final KafkaTemplate<String, String> template;

    public OrderTransfer(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    /** @param fail demo only: throw after sending, to show the rollback */
    @Transactional
    public void transfer(String batch, boolean fail) {
        for (int i = 1; i <= 3; i++) {
            template.send(TopicsConfig.TXN_OUT, batch + "-" + i, "transfer " + batch + " part " + i);
        }
        if (fail) {
            template.flush();   // demo only: let the records reach the log, so the rollback leaves visible abort markers
            throw new IllegalStateException("rolled back by script: " + batch);
        }
    }
}
