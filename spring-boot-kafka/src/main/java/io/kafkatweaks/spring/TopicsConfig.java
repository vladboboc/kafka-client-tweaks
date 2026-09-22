package io.kafkatweaks.spring;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * The topics of part 2, provisioned by the auto-configured {@link KafkaAdmin} when the context starts
 * ({@code spring.kafka.admin.auto-create=true}, the default): every {@link NewTopic} bean, or a
 * {@link KafkaAdmin.NewTopics} bundle like this one, is created if missing. Existing topics are left alone
 * (partition counts are never lowered; configs only change with {@code modify-topic-configs=true}).
 * <p>
 * The plain chapters get their topics from {@code docker/kafka/create-topics.sh}; here the application owns them,
 * which is what most Spring services do. One caveat carried over from chapter 07: creation returns before the
 * partitions have leaders, so the demos still call {@code Topics.ensure(...)} (which waits) before seeding.
 */
@Configuration(proxyBeanMethods = false)
public class TopicsConfig {

    public static final String TEMPLATE = "spring.template";
    public static final String LISTENER = "spring.listener";
    public static final String PARALLEL = "spring.parallel";
    public static final String ERRORS = "spring.errors";
    /** Default destination of DeadLetterPublishingRecoverer: the topic name + "-dlt". */
    public static final String ERRORS_DLT = "spring.errors-dlt";
    /** @RetryableTopic creates its own -retry-* and -dlt topics next to this one. */
    public static final String RETRYABLE = "spring.retryable";
    public static final String TXN_IN = "spring.txn-in";
    public static final String TXN_OUT = "spring.txn-out";
    public static final String QUEUE = "spring.queue";
    /** One partition, so that the consumer threads of a lock demo are necessarily fed from the same partition (created by the demo). */
    public static final String QUEUE_LOCKS = "spring.queue-locks";
    public static final String SERDES = "spring.serdes";
    public static final String AVRO = "spring.avro";

    /** Replication factor of the compose stack (3 brokers, min.insync.replicas=2). */
    public static final int REPLICAS = 3;

    @Bean
    KafkaAdmin.NewTopics springTopics() {
        return new KafkaAdmin.NewTopics(
                topic(TEMPLATE, 3), topic(LISTENER, 3), topic(PARALLEL, 6),
                topic(ERRORS, 3), topic(ERRORS_DLT, 3), topic(RETRYABLE, 3),
                topic(TXN_IN, 3), topic(TXN_OUT, 3), topic(QUEUE, 3),
                topic(SERDES, 3), topic(AVRO, 3));
    }

    private static NewTopic topic(String name, int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(REPLICAS).build();
    }
}
