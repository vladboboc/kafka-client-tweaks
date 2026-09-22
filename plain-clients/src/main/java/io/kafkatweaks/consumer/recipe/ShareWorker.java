package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Chapter 11 · Queues for Kafka (KIP-932): records, not partitions, are the unit of work.
 * <p>
 * A share group hands records of the same partition to any number of consumers, and each record is acknowledged on
 * its own: {@code ACCEPT} done, {@code RELEASE} give it back for another try, {@code REJECT} never again. The broker
 * counts deliveries and archives a record after {@code share.delivery.count.limit} of them. Measured by the
 * {@code consumer-share} demo (docs/11-consumer-share-groups.md): 4 consumers on 3 partitions all got work; with
 * every 50th record rejected and every 7th released once, 3 000 input records ended as 2 940 accepted + 60 rejected,
 * and the 420 released ones came back with deliveryCount 2.
 * <pre>{@code
 * ShareWorker.configureGroup(admin, "jobs", ShareWorker.groupSettings("earliest", Duration.ofSeconds(30), 5));
 * props.putAll(ShareWorker.explicitAcks());
 * try (var consumer = new KafkaShareConsumer<String, String>(props)) {
 *     consumer.subscribe(List.of("jobs"));
 *     var worker = new ShareWorker<>(consumer, record -> isPoison(record) ? REJECT : run(record) ? ACCEPT : RELEASE,
 *             (partition, error) -> log.warn("acks for {} not applied", partition, error));
 *     while (running) {
 *         worker.pollOnce(Duration.ofMillis(500));
 *     }
 * }
 * }</pre>
 * No ordering across members, by design: if two records of one key must not run concurrently, use a consumer group.
 */
public final class ShareWorker<K, V> {

    /** Your decision for one record. Throwing counts as {@code RELEASE}: the record will be delivered again. */
    @FunctionalInterface
    public interface Decider<K, V> {
        AcknowledgeType decide(ConsumerRecord<K, V> record) throws Exception;
    }

    /**
     * Acknowledge every record yourself. The default, {@code implicit}, ACCEPTs everything a poll returned on the next
     * {@code poll()} or {@code commitSync()}: simple, and the last poll before shutdown must be committed, or those
     * records come back to someone else once their lock expires.
     */
    public static Map<String, Object> explicitAcks() {
        return Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
    }

    /**
     * The GROUP settings of a share group. They are not client configs: set them once per group, with
     * {@link #configureGroup} or {@code kafka-configs --entity-type groups --entity-name <group> --alter --add-config ...}.
     *
     * @param autoOffsetReset where a NEW share group starts: {@code latest} (default) or {@code earliest}
     * @param lockDuration    how long a delivered record stays locked to its consumer (default 30 s). Size it above your
     *                        slowest honest handler, or acknowledge with {@code AcknowledgeType.RENEW} from long work
     * @param deliveryLimit   deliveries before the broker archives a record (default 5): "retry N times, then give up"
     */
    public static Map<String, String> groupSettings(String autoOffsetReset, Duration lockDuration, int deliveryLimit) {
        return Map.of(
                "share.auto.offset.reset", autoOffsetReset,
                "share.record.lock.duration.ms", String.valueOf(lockDuration.toMillis()),
                "share.delivery.count.limit", String.valueOf(deliveryLimit));
    }

    /** Sets group-level configs; the config entry is created even before the group exists. */
    public static void configureGroup(Admin admin, String groupId, Map<String, String> settings)
            throws ExecutionException, InterruptedException {
        var group = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        List<AlterConfigOp> ops = settings.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        admin.incrementalAlterConfigs(Map.of(group, ops)).all().get();
    }

    private final ShareConsumer<K, V> consumer;
    private final Decider<K, V> decider;
    private final BiConsumer<TopicIdPartition, KafkaException> onCommitError;

    /** @param onCommitError called per partition whose acknowledgements the broker did not apply (e.g. the lock had expired) */
    public ShareWorker(ShareConsumer<K, V> consumer, Decider<K, V> decider, BiConsumer<TopicIdPartition, KafkaException> onCommitError) {
        this.consumer = consumer;
        this.decider = decider;
        this.onCommitError = onCommitError;
    }

    /**
     * One turn of the loop: poll, decide and acknowledge every record, then send the acknowledgements.
     *
     * @return the number of records this poll returned; 0 means nothing was available within {@code timeout}
     */
    public int pollOnce(Duration timeout) {
        ConsumerRecords<K, V> batch = consumer.poll(timeout);
        if (batch.isEmpty()) {
            return 0;
        }
        for (ConsumerRecord<K, V> record : batch) {
            AcknowledgeType outcome;
            try {
                outcome = decider.decide(record);   // record.deliveryCount() says how many times it was delivered before
            } catch (Exception e) {
                outcome = AcknowledgeType.RELEASE;   // retry; after share.delivery.count.limit deliveries it is archived
            }
            consumer.acknowledge(record, outcome);
        }
        // Sends the acknowledgements and reports the outcome per partition; an error means those acks were not applied.
        consumer.commitSync().forEach((partition, error) -> error.ifPresent(e -> onCommitError.accept(partition, e)));
        return batch.count();
    }
}
