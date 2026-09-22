package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Chapter 12 · When the cluster misbehaves: read from the local rack, ride through a dying broker, see every client.
 * <p>
 * Measured by the {@code client-resilience} demo (docs/12-client-resilience.md):
 * <pre>
 *   rackAware("rack-b")            238.3K of 240K bytes fetched from the rack-b broker; without it, from each leader
 *   outageTolerantProducer()       broker stopped for 8 s at 1 500 records/s: 36 000 sent, 36 000 in the topic,
 *                                  0 failed, 0 duplicates, 19 records retried
 * </pre>
 * The interceptors of this chapter are {@link StampingProducerInterceptor} and {@link LatencyConsumerInterceptor}.
 */
public final class ResilientClients {

    private ResilientClients() {
    }

    /**
     * Consumers: fetch from the replica in your own rack or zone (follower fetching, KIP-392), whether or not it leads
     * the partition. Needs {@code broker.rack} on the brokers and
     * {@code replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector}. Cuts cross-zone traffic;
     * a follower can be a replication hop behind the leader.
     */
    public static Map<String, Object> rackAware(String rack) {
        return Map.of(CommonClientConfigs.CLIENT_RACK_CONFIG, rack);
    }

    /**
     * The settings that make a broker restart invisible to a producer. They ARE the 4.x defaults: written out here to
     * name them, and as a warning not to "tune" them down.
     */
    public static Map<String, Object> outageTolerantProducer() {
        return Map.of(
                ProducerConfig.ACKS_CONFIG, "all",                         // complete: every in-sync replica has it
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,            // safe: a retried batch is stored once, in order
                ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,          // retry until the delivery budget is spent ...
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000,        // ... which must outlast leader election + metadata refresh
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100L,              // first pause between retries, doubling ...
                ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, 1000L,         // ... up to this. Raise both on fleets of thousands
                CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, 50L,      // same idea per broker connection, so a returning
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000L,   // broker is not stampeded by reconnects
                // When EVERY known broker is gone (a replaced node pool), re-resolve bootstrap.servers instead of spinning
                // on stale metadata. Only works if bootstrap.servers is a stable DNS name, a load balancer or all brokers.
                CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
    }

    /**
     * The id the broker gave this client for KIP-714 telemetry: ties its metrics, logs and quotas to one process. Empty
     * until the telemetry handshake, which runs in the background, has completed: a client that has lived for half a
     * second usually has none yet, a long-running one does.
     */
    public static Optional<Uuid> instanceId(Producer<?, ?> producer, Duration timeout) {
        return Optional.ofNullable(producer.clientInstanceId(timeout));
    }
}
