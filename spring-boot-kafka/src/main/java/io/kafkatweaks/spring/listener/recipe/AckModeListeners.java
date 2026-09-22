package io.kafkatweaks.spring.listener.recipe;

import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.listener.ListenerProbe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Chapter 16 · When offsets are committed is one attribute of {@code @KafkaListener}: {@code ackMode} (spring-kafka 4.1;
 * the factory default comes from {@code spring.kafka.listener.ack-mode}, BATCH). Every listener here reads the whole
 * topic in its own group, so the commit counts of the demo are the effect of that attribute alone. Measured by the
 * {@code spring-listener-acks} demo (docs/16-spring-listeners-acks.md), 6 000 records:
 * <pre>
 *   RECORD              6 000 commits   15 815 ms   a synchronous commit per record
 *   BATCH                  12 commits      109 ms   one per poll: the default, and the sweet spot
 *   TIME / COUNT          1 / 6 commits  ~110 ms
 *   MANUAL_IMMEDIATE       12 commits      157 ms   when the listener calls acknowledge()
 * </pre>
 * {@code probe.*} calls are the demo's measurement: your processing goes there.
 */
@Component
@Profile("spring-listener-acks")
public class AckModeListeners {

    private final ListenerProbe probe;

    public AckModeListeners(ListenerProbe probe) {
        this.probe = probe;
    }

    // ---- 1. one listener per ack mode ---------------------------------------------------------------------

    @KafkaListener(id = "acks-record", groupId = "spring-acks-record", clientIdPrefix = "acks-record", topics = TopicsConfig.LISTENER, ackMode = "RECORD")
    public void record(ConsumerRecord<String, String> record) {
        probe.hit("acks-record");
    }

    @KafkaListener(id = "acks-batch", groupId = "spring-acks-batch", clientIdPrefix = "acks-batch", topics = TopicsConfig.LISTENER, ackMode = "BATCH")
    public void batch(ConsumerRecord<String, String> record) {
        probe.hit("acks-batch");
    }

    @KafkaListener(id = "acks-time", groupId = "spring-acks-time", clientIdPrefix = "acks-time", topics = TopicsConfig.LISTENER, ackMode = "TIME")
    public void time(ConsumerRecord<String, String> record) {
        probe.hit("acks-time");
    }

    @KafkaListener(id = "acks-count", groupId = "spring-acks-count", clientIdPrefix = "acks-count", topics = TopicsConfig.LISTENER, ackMode = "COUNT")
    public void count(ConsumerRecord<String, String> record) {
        probe.hit("acks-count");
    }

    @KafkaListener(id = "acks-manual", groupId = "spring-acks-manual", clientIdPrefix = "acks-manual", topics = TopicsConfig.LISTENER, ackMode = "MANUAL_IMMEDIATE")
    public void manual(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (probe.hitAndCount("acks-manual") % 500 == 0) {
            ack.acknowledge();   // commits the offset of THIS record immediately (MANUAL would defer to the end of the poll)
        }
    }

    // ---- 2. nack: manual assignment of partition 0 from offset 0, small polls, one negative acknowledgement ---

    @KafkaListener(id = "acks-nack", groupId = "spring-acks-nack", clientIdPrefix = "acks-nack", ackMode = "MANUAL",
            properties = "max.poll.records:5",
            topicPartitions = @TopicPartition(topic = TopicsConfig.LISTENER,
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")))
    public void nack(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (probe.nackOnce(record)) {   // the demo's script: offset 3, first delivery only
            ack.nack(Duration.ofSeconds(1));   // commit what was acked, drop the rest of this poll, seek back to this record, pause 1 s
        } else {
            ack.acknowledge();
        }
    }

    // ---- 4. a filtered listener (the RecordFilterStrategy bean is picked by name via the filter attribute) ----

    @KafkaListener(id = "acks-filter", groupId = "spring-acks-filter", clientIdPrefix = "acks-filter", topics = TopicsConfig.LISTENER, filter = "oddOffsetFilter")
    public void filtered(ConsumerRecord<String, String> record) {
        probe.hit("acks-filter");
    }
}
