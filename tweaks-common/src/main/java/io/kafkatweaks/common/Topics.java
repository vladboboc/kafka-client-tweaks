package io.kafkatweaks.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Thin wrapper over {@link Admin} for the chores every demo needs: create a topic with a deliberate
 * partition count, wipe a topic or a consumer group between runs, and look at assignments, lag and
 * partition leadership so the effect of a tweak can be shown rather than asserted.
 * <p>
 * Brokers run with {@code auto.create.topics.enable=false}, so nothing here is optional.
 */
public final class Topics implements AutoCloseable {

    private static final Duration DELETE_WAIT = Duration.ofSeconds(30);
    /** How long a topic that must exist may stay unknown to the broker a describe lands on (see describeExisting). */
    private static final Duration METADATA_LAG = Duration.ofSeconds(10);
    private final Admin admin;

    public Topics() {
        this.admin = Admin.create(Env.admin());
    }

    public Admin admin() {
        return admin;
    }

    /** Creates the topic if it does not exist (RF=3, the cluster default). Existing topics are left alone. */
    public void ensure(String topic, int partitions, Map<String, String> configs) {
        ensure(topic, partitions, 3, configs);
    }

    /** Same, with an explicit replication factor (chapter 03 needs RF=2 topics). */
    public void ensure(String topic, int partitions, int replicationFactor, Map<String, String> configs) {
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) replicationFactor).configs(configs))).all().get();
            System.out.printf("topic %s created (%d partitions, RF=%d%s)%n", topic, partitions, replicationFactor, describeConfigs(configs));
            // createTopics() returns when the controller has accepted the topic; the brokers' metadata catches up a
            // moment later. Anything that describes or writes the topic right away would see UNKNOWN_TOPIC_OR_PARTITION.
            // This loop only proves it for the broker that answered; describeExisting() covers a later read that
            // lands on one still catching up.
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (describe(topic).map(d -> d.partitions().stream().anyMatch(p -> p.leader() == null)).orElse(true)) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("topic " + topic + " not visible 15 s after creation");
                }
                sleep(100);
            }
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("cannot create topic " + topic, e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public void ensure(String topic, int partitions) {
        ensure(topic, partitions, Map.of());
    }

    /** Deletes the topic if present, waits for the deletion to complete, then creates it fresh. */
    public void recreate(String topic, int partitions, Map<String, String> configs) {
        delete(topic);
        ensure(topic, partitions, configs);
    }

    public void recreate(String topic, int partitions) {
        recreate(topic, partitions, Map.of());
    }

    public void delete(String topic) {
        try {
            admin.deleteTopics(List.of(topic)).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return;
            }
            throw new IllegalStateException("cannot delete topic " + topic, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        // Deletion is asynchronous on the broker; creating the same name too early fails with TopicExists.
        long deadline = System.nanoTime() + DELETE_WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (describe(topic).isEmpty()) {
                return;
            }
            sleep(200);
        }
        throw new IllegalStateException("topic " + topic + " still present " + DELETE_WAIT + " after delete");
    }

    /**
     * Empty when the broker asked does not know the topic. Right after a create that broker may just be catching up,
     * so a caller that knows the topic exists wants {@link #describeExisting} instead.
     */
    public Optional<TopicDescription> describe(String topic) {
        try {
            return Optional.of(admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return Optional.empty();
            }
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * The description of a topic the caller knows exists, typically one it has just created. Every broker applies a
     * metadata change on its own, and describeTopics() goes to whichever broker the Admin client finds least loaded, not
     * to the one that answered last: once the client is connected to several brokers, one of them can describe a new
     * topic with its leaders while the next one asked still replies UNKNOWN_TOPIC_OR_PARTITION (for tens of
     * milliseconds on a busy stack; it failed chapter 20 once). So an unknown topic is asked about again before it
     * counts as missing.
     */
    public TopicDescription describeExisting(String topic) {
        long deadline = System.nanoTime() + METADATA_LAG.toNanos();
        while (true) {
            Optional<TopicDescription> description = describe(topic);
            if (description.isPresent()) {
                return description.get();
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("topic " + topic + " does not exist");
            }
            sleep(100);
        }
    }

    public int partitionCount(String topic) {
        return describeExisting(topic).partitions().size();
    }

    /**
     * Deletes a consumer group so the next run starts from {@code auto.offset.reset}; unknown groups are ignored.
     * Members of a crashed previous run linger until their session times out, so this waits up to a minute for them.
     */
    public void deleteGroup(String groupId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try {
                admin.deleteConsumerGroups(List.of(groupId)).all().get();
                System.out.printf("consumer group %s deleted%n", groupId);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof GroupIdNotFoundException) {
                    return;
                }
                if (e.getCause() instanceof GroupNotEmptyException && System.nanoTime() < deadline) {
                    System.out.printf("consumer group %s still has members (a previous run did not leave cleanly); waiting for the session timeout...%n", groupId);
                    sleep(3000);
                    continue;
                }
                throw new IllegalStateException("cannot delete group " + groupId, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Deletes a share group (KIP-932) and with it the per-record state it holds: delivery counts, acquired and
     * archived records. Like {@link #deleteGroup}, it waits for members of a crashed previous run to time out.
     */
    public void deleteShareGroup(String groupId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try {
                admin.deleteShareGroups(List.of(groupId)).all().get();
                System.out.printf("share group %s deleted%n", groupId);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof GroupIdNotFoundException) {
                    return;
                }
                if (e.getCause() instanceof GroupNotEmptyException && System.nanoTime() < deadline) {
                    System.out.printf("share group %s still has members (a previous run did not leave cleanly); waiting for the session timeout...%n", groupId);
                    sleep(3000);
                    continue;
                }
                throw new IllegalStateException("cannot delete share group " + groupId, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Sets group-level configs (the {@code kafka-configs --entity-type groups} equivalent), e.g. the
     * {@code share.*} settings of a share group. Kafka creates the config entry even if the group does not exist yet.
     */
    public void alterGroupConfigs(String groupId, Map<String, String> configs) {
        var resource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Collection<AlterConfigOp> ops = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        try {
            admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("cannot alter configs of group " + groupId, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Which member of the group owns which partitions right now, keyed by client id. */
    public Map<String, List<TopicPartition>> assignments(String groupId) {
        try {
            ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(groupId)).all().get().get(groupId);
            var out = new LinkedHashMap<String, List<TopicPartition>>();
            d.members().forEach(m -> out.put(m.clientId(),
                    m.assignment().topicPartitions().stream()
                            .sorted((a, b) -> a.partition() - b.partition())
                            .toList()));
            return out;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return Map.of();
            }
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Per-partition lag: log end offset minus committed offset (uncommitted partitions count from 0). */
    public Map<TopicPartition, Long> lag(String groupId) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) {
                return Map.of();
            }
            Map<TopicPartition, OffsetSpec> latest = committed.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(latest).all().get();
            var out = new LinkedHashMap<TopicPartition, Long>();
            for (var e : ends.entrySet()) {
                OffsetAndMetadata c = committed.get(e.getKey());
                out.put(e.getKey(), e.getValue().offset() - (c == null ? 0 : c.offset()));
            }
            return out;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Log end offsets for every partition of a topic. */
    public Map<TopicPartition, Long> endOffsets(String topic) {
        try {
            int n = partitionCount(topic);
            var spec = new HashMap<TopicPartition, OffsetSpec>();
            for (int p = 0; p < n; p++) {
                spec.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
            var out = new LinkedHashMap<TopicPartition, Long>();
            admin.listOffsets(spec).all().get().forEach((tp, info) -> out.put(tp, info.offset()));
            return out;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Prints partition → leader / replicas / ISR, the view the durability chapter reasons about. */
    public void printPartitions(String topic) {
        TopicDescription d = describeExisting(topic);
        var table = new Table("partition", "leader", "replicas", "in-sync replicas");
        d.partitions().forEach(p -> table.row(
                p.partition(),
                p.leader() == null ? "none" : "broker " + p.leader().id(),
                ids(p.replicas().stream().map(n -> n.id()).toList()),
                ids(p.isr().stream().map(n -> n.id()).toList())));
        table.print("topic " + topic);
    }

    public void printAssignments(String groupId) {
        var table = new Table("member (client.id)", "partitions");
        assignments(groupId).forEach((client, tps) -> table.row(client,
                tps.stream().map(tp -> String.valueOf(tp.partition())).collect(Collectors.joining(","))));
        table.print("assignments of group " + groupId);
    }

    public void printLag(String groupId) {
        var table = new Table("partition", "lag");
        long total = 0;
        for (var e : lag(groupId).entrySet()) {
            table.row(e.getKey().topic() + "-" + e.getKey().partition(), e.getValue());
            total += e.getValue();
        }
        table.row("total", total);
        table.print("lag of group " + groupId);
    }

    private static String ids(List<Integer> ids) {
        var copy = new ArrayList<>(ids);
        copy.sort(null);
        return copy.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    private static String describeConfigs(Map<String, String> configs) {
        return configs.isEmpty() ? "" : ", " + configs.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
