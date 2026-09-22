package io.kafkatweaks.producer.recipe;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.Map;

/**
 * Chapter 03 · Durability: what "the send succeeded" promises, and how long a refused write takes to fail.
 * <p>
 * {@code acks=all} is only as strong as the topic's {@code min.insync.replicas}. Measured by the
 * {@code producer-durability} demo (docs/03-producer-durability.md) on an RF=2 topic with
 * {@code min.insync.replicas=2} and one broker stopped:
 * <pre>
 *   durable()    + failFast(3 s, 8 s, 10 s)   FAIL after 8102 ms: TimeoutException (NOT_ENOUGH_REPLICAS, retried)
 *   leaderOnly() + failFast(3 s, 8 s, 10 s)   OK after 138 ms, on ONE disk: lost if that broker dies now
 * </pre>
 * On a healthy cluster all {@code acks} settings cost about the same; they differ in what happens when something fails.
 */
public final class DurableProducer {

    private DurableProducer() {
    }

    /** The 4.x default, written out: nothing acknowledged is lost while fewer than min.insync.replicas brokers fail. */
    public static Map<String, Object> durable() {
        return Map.of(
                // The leader answers once every in-sync replica has the batch (at least min.insync.replicas of them).
                ProducerConfig.ACKS_CONFIG, "all",
                // Producer id + a sequence number per partition: a retried batch is stored once, and up to 5 requests
                // in flight stay in order. Costs nothing measurable; it is what makes the infinite retries safe.
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    }

    /** Leader-only acknowledgement: lower latency, and the record is lost if the leader dies before a follower copied it. */
    public static Map<String, Object> leaderOnly() {
        return Map.of(
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);   // idempotence needs acks=all: the client refuses acks=1 with it on
    }

    /** No acknowledgement at all: the record counts as sent once it left the socket. Metrics and logs where a gap is fine. */
    public static Map<String, Object> fireAndForget() {
        return Map.of(
                ProducerConfig.ACKS_CONFIG, "0",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    }

    /**
     * Fail in seconds instead of minutes, and let the caller decide. The producer retries retriable errors (like
     * NOT_ENOUGH_REPLICAS) until the delivery timeout runs out; {@code retries} is effectively infinite and not the knob
     * to turn. The client refuses to start unless {@code deliveryTimeout >= linger.ms + requestTimeout}.
     * Keep the defaults (120 s) to ride through a broker restart without the application ever seeing an error.
     */
    public static Map<String, Object> failFast(Duration requestTimeout, Duration deliveryTimeout, Duration maxBlock) {
        return Map.of(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis(),     // one produce request, default 30 s
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) deliveryTimeout.toMillis(),   // send() to callback, all retries, default 120 s
                ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlock.toMillis());                      // send() blocking on metadata or buffer, default 60 s
    }

    /**
     * A TOPIC setting, not a producer one: the floor for {@code acks=all}. With fewer in-sync replicas the leader
     * refuses the write. RF=3 + min.insync.replicas=2 survives one broker; RF=2 + 2 survives none; RF=3 + 1 can
     * acknowledge a record that lives on a single disk. Pass it when creating the topic (Admin {@code NewTopic.configs}).
     */
    public static Map<String, String> minInSyncReplicas(int replicas) {
        return Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(replicas));
    }
}
