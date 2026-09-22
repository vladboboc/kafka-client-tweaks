package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

import java.util.Map;

/**
 * Chapter 09 · Rebalancing: move only the partitions that have to move.
 * <p>
 * kafka-clients 4.3 still defaults to {@code group.protocol=classic} (deprecated by KIP-1274), and classic with the
 * eager {@code RangeAssignor} revokes EVERY partition from EVERY member on every join, leave and deploy. Measured by
 * the {@code consumer-rebalance} demo (docs/09-consumer-rebalance.md), 6 partitions, partitions member A gave up when
 * B joined / when C joined:
 * <pre>
 *   classic + RangeAssignor (eager)       6 / 3   the whole group stops on every change
 *   consumerProtocol()                    3 / 1   the other partitions keep being consumed
 *   classicCooperative()                  3 / 1   same shape, two rebalance rounds per change
 *   consumerProtocol() + staticMember()   B restarts within the session timeout: gets [3,4] back, A and C see nothing
 * </pre>
 */
public final class GroupProtocols {

    private GroupProtocols() {
    }

    /**
     * The KIP-848 protocol, GA since 4.0: the broker computes the assignment and members learn about changes through
     * heartbeats, always incrementally. The first choice for a new application on Kafka 4.x brokers.
     * {@code session.timeout.ms} and {@code heartbeat.interval.ms} become BROKER settings
     * ({@code group.consumer.session.timeout.ms}, {@code group.consumer.heartbeat.interval.ms}); the client values are ignored.
     */
    public static Map<String, Object> consumerProtocol() {
        return Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
    }

    /** The same, with a named server-side assignor: {@code uniform} (default) or {@code range} for co-partitioned topics. */
    public static Map<String, Object> consumerProtocol(String remoteAssignor) {
        return Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer",
                ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, remoteAssignor);
    }

    /** Stuck on the classic protocol for now: at least make it incremental and sticky. */
    public static Map<String, Object> classicCooperative() {
        return Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    }

    /**
     * Static membership: {@code close()} is not a leave. The coordinator keeps this member's partitions for it until the
     * session timeout, so a restart gets the same partitions back without moving anything. For stateful consumers and
     * rolling deploys; the price is idle partitions for a whole session timeout if the instance never comes back.
     *
     * @param instanceId stable per instance and unique in the group: a pod ordinal, a host name
     */
    public static Map<String, Object> staticMember(String instanceId) {
        return Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
    }
}
