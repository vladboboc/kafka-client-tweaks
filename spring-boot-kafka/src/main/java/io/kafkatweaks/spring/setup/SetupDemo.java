package io.kafkatweaks.spring.setup;

import io.kafkatweaks.common.Knobs;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.List;
import java.util.Map;

/**
 * Chapter 14: what Spring Boot builds from {@code spring.kafka.*}, and how a YAML key becomes a client config.
 * <ol>
 *   <li>the beans Boot auto-configured (and which ones it did not, and why)</li>
 *   <li>the properties of application-spring-setup.yml next to the client configs they became</li>
 *   <li>the shared {@code Knobs} printer on the auto-configured factories: this run vs client default</li>
 *   <li>the topics KafkaAdmin created from the {@code NewTopics} bean</li>
 *   <li>versions: Boot, spring-kafka, kafka-clients here vs in plain-clients, the brokers</li>
 * </ol>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-setup")
public class SetupDemo {

    @Bean
    ApplicationRunner springSetup(DemoSupport support, ApplicationContext context, ProducerFactory<?, ?> producerFactory,
                                  ConsumerFactory<?, ?> consumerFactory, KafkaAdmin admin, KafkaProperties properties) {
        return support.demo("spring-setup", args -> {
            // ---- 1. beans -------------------------------------------------------------------------------------
            var beans = new Table("bean type", "in this context", "who creates it");
            row(beans, context, KafkaTemplate.class, "KafkaAutoConfiguration, spring.kafka.template.*");
            row(beans, context, ProducerFactory.class, "KafkaAutoConfiguration, spring.kafka.producer.*");
            row(beans, context, ConsumerFactory.class, "KafkaAutoConfiguration, spring.kafka.consumer.*");
            row(beans, context, KafkaAdmin.class, "KafkaAutoConfiguration, spring.kafka.admin.*");
            row(beans, context, ProducerListener.class, "KafkaAutoConfiguration: LoggingProducerListener unless you define one");
            row(beans, context, KafkaListenerContainerFactory.class, "KafkaAnnotationDrivenConfiguration, spring.kafka.listener.*");
            row(beans, context, KafkaListenerEndpointRegistry.class, "@EnableKafka (implied): one container per @KafkaListener");
            row(beans, context, KafkaTransactionManager.class, "only when spring.kafka.producer.transaction-id-prefix is set (ch. 19)");
            row(beans, context, CommonErrorHandler.class, "you; otherwise every container gets its own DefaultErrorHandler (ch. 18)");
            row(beans, context, MeterRegistry.class, "spring-boot-starter-micrometer-metrics; Boot then binds the client metrics");
            beans.print("1. beans Boot auto-configured (declare a bean of the same type and Boot backs off)");

            // ---- 2. property -> config ------------------------------------------------------------------------
            Map<String, Object> producer = producerFactory.getConfigurationProperties();
            Map<String, Object> consumer = consumerFactory.getConfigurationProperties();
            Map<String, Object> adminConfigs = admin.getConfigurationProperties();
            var mapping = new Table("spring.kafka.* (application.yml + application-spring-setup.yml)", "client config", "producer", "consumer", "admin");
            mapping.row("bootstrap-servers", "bootstrap.servers", v(producer, "bootstrap.servers"), v(consumer, "bootstrap.servers"), v(adminConfigs, "bootstrap.servers"));
            mapping.row("client-id: spring-tweaks", "client.id", v(producer, "client.id"), v(consumer, "client.id"), v(adminConfigs, "client.id"));
            mapping.row("properties[metadata.max.age.ms]: 30000", "metadata.max.age.ms", v(producer, "metadata.max.age.ms"), v(consumer, "metadata.max.age.ms"), v(adminConfigs, "metadata.max.age.ms"));
            mapping.row("producer.acks: all", "acks", v(producer, "acks"), "", "");
            mapping.row("producer.batch-size: 64KB", "batch.size", v(producer, "batch.size"), "", "");
            mapping.row("producer.compression-type: zstd", "compression.type", v(producer, "compression.type"), "", "");
            mapping.row("producer.properties[linger.ms]: 20", "linger.ms", v(producer, "linger.ms"), "", "");
            mapping.row("producer.key-serializer (default)", "key.serializer", v(producer, "key.serializer"), "", "");
            mapping.row("consumer.group-id: spring-setup", "group.id", "", v(consumer, "group.id"), "");
            mapping.row("consumer.auto-offset-reset: earliest", "auto.offset.reset", "", v(consumer, "auto.offset.reset"), "");
            mapping.row("consumer.max-poll-records: 250", "max.poll.records", "", v(consumer, "max.poll.records"), "");
            mapping.row("consumer.fetch-max-wait: 250ms", "fetch.max.wait.ms", "", v(consumer, "fetch.max.wait.ms"), "");
            mapping.row("consumer.isolation-level: read_committed", "isolation.level", "", v(consumer, "isolation.level"), "");
            mapping.row("consumer.properties[group.protocol]: consumer", "group.protocol", "", v(consumer, "group.protocol"), "");
            mapping.print("2. how the YAML arrived in the clients (typed keys are converted: 64KB -> 65536, 250ms -> 250)");
            System.out.println("   precedence: spring.kafka.properties < spring.kafka.<client>.properties, and a typed key and its properties[...] twin");
            System.out.println("   should not both be set. Anything can be overridden on the command line: --spring.kafka.producer.properties.linger.ms=50");

            // ---- 3. this run vs client default, with the same printer the plain chapters use -----------------
            Knobs.printProducer(DemoSupport.toProperties(producer), ProducerConfig.ACKS_CONFIG, ProducerConfig.BATCH_SIZE_CONFIG,
                    ProducerConfig.LINGER_MS_CONFIG, ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
            Knobs.printConsumer(DemoSupport.toProperties(consumer), ConsumerConfig.GROUP_PROTOCOL_CONFIG, ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, ConsumerConfig.FETCH_MIN_BYTES_CONFIG, ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
            System.out.println("   enable.auto.commit is untouched here on purpose: the listener CONTAINER sets it to false when it creates");
            System.out.println("   its consumer and commits offsets itself (chapter 16).");

            KafkaProperties.Listener listener = properties.getListener();
            var containers = new Table("spring.kafka.listener.*", "value", "meaning");
            containers.row("type", listener.getType(), "SINGLE = one record per listener call, BATCH = the whole poll (ch. 17)");
            containers.row("ack-mode", listener.getAckMode() == null ? ContainerProperties.AckMode.BATCH + " (default)" : listener.getAckMode(), "when the container commits (ch. 16)");
            containers.row("concurrency", listener.getConcurrency() == null ? "1 (default)" : listener.getConcurrency(), "consumers per @KafkaListener (ch. 17)");
            containers.row("auto-startup", listener.isAutoStartup(), "false in this repo: demos seed first, then start their listeners");
            containers.row("poll-timeout", listener.getPollTimeout() == null ? "5s (default)" : listener.getPollTimeout(), "the Duration handed to consumer.poll()");
            containers.print("3b. the listener container factory's own knobs (they are not client configs)");

            // ---- 4. topics -----------------------------------------------------------------------------------
            try (var topics = new Topics()) {
                topics.printPartitions(TopicsConfig.TEMPLATE);
                topics.printPartitions(TopicsConfig.PARALLEL);
            }
            System.out.println("   4. created by KafkaAdmin from the NewTopics bean in TopicsConfig when the context started");
            System.out.println("   (spring.kafka.admin.auto-create=true). Creation returns before leaders are elected, hence Topics.ensure() before seeding.");

            // ---- 5. versions ---------------------------------------------------------------------------------
            var versions = new Table("component", "version", "decided by");
            versions.row("Spring Boot", SpringBootVersion.getVersion(), "spring-boot.version in the root pom");
            versions.row("spring-kafka", String.valueOf(KafkaTemplate.class.getPackage().getImplementationVersion()), "Boot's BOM");
            versions.row("kafka-clients in this module", AppInfoParser.getVersion(), "Boot's BOM: what spring-kafka is compiled against");
            versions.row("kafka-clients in plain-clients", "4.3.1", "kafka.version in the root pom: the brokers' line");
            versions.row("brokers", "Confluent Platform 8.3.2 = Apache Kafka 4.3", "docker-compose.yml");
            versions.print("5. versions (a 4.2 client against 4.3 brokers is a supported combination; 4.3-only client behaviour is absent here)");
        });
    }

    private static void row(Table table, ApplicationContext context, Class<?> type, String origin) {
        String[] names = context.getBeanNamesForType(type);
        table.row(type.getSimpleName(), names.length == 0 ? "-" : String.join(", ", names), origin);
    }

    private static String v(Map<String, Object> configs, String key) {
        Object value = configs.get(key);
        return switch (value) {
            case null -> "";
            case List<?> l -> String.join(",", l.stream().map(String::valueOf).toList());
            case Class<?> c -> c.getSimpleName();
            default -> String.valueOf(value);
        };
    }
}
