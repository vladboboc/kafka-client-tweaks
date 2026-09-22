package io.kafkatweaks.spring.txn;

import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.txn.recipe.OrderTransfer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A service that only sends needs no broker and no Spring context: a {@code KafkaTemplate} over spring-kafka's
 * {@link MockProducerFactory} hands it kafka-clients' {@link MockProducer}, whose {@code history()} is the assertion.
 * <p>
 * One trap: {@code KafkaTemplate} closes the producer it borrowed after every non-transactional send. A real factory's
 * producers ignore that (they go back to the cache); a {@code MockProducer} really closes and refuses the next send,
 * so the mock here overrides {@code close(Duration)}.
 */
class MockProducerFactoryTest {

    static final class RecordingProducer extends MockProducer<String, String> {
        RecordingProducer() {
            super(true, null, new StringSerializer(), new StringSerializer());   // autoComplete: every send succeeds at once
        }

        @Override
        public void close(Duration timeout) {
            // keep the history across the template's per-send close()
        }
    }

    private final RecordingProducer producer = new RecordingProducer();
    private final KafkaTemplate<String, String> template = new KafkaTemplate<>(new MockProducerFactory<>(() -> producer));
    private final OrderTransfer transfer = new OrderTransfer(template);

    @Test
    void aTransferSendsThreeRecordsToTheOutputTopic() {
        transfer.transfer("t1", false);

        assertThat(producer.history()).hasSize(3)
                .allSatisfy(record -> assertThat(record.topic()).isEqualTo(TopicsConfig.TXN_OUT))
                .extracting(ProducerRecord::key).containsExactly("t1-1", "t1-2", "t1-3");
        assertThat(producer.history().getFirst().value()).isEqualTo("transfer t1 part 1");
    }

    @Test
    void aFailingTransferSendsBeforeItThrows() {
        assertThatThrownBy(() -> transfer.transfer("t2", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("t2");

        // The records were sent; without a transaction manager around the call nothing takes them back.
        // What @Transactional adds (a rollback, abort markers) is chapter 19's demo, not a unit test's job.
        assertThat(producer.history()).hasSize(3);
        assertThat(producer.flushed()).isTrue();
    }
}
