package io.kafkatweaks.spring;

import io.kafkatweaks.common.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;
import java.util.Optional;

/**
 * The demos of part 2 in chapter order. The name is also the Spring profile and the {@code application-<name>.yml} file.
 * A run that names no demo, or an unknown one, gets the {@value #PROFILE} profile instead: the runner below lists the
 * demos through the same logging as everything else, and application-catalogue.yml keeps that run broker-free.
 */
@Configuration(proxyBeanMethods = false)
@Profile(Catalogue.PROFILE)
public class Catalogue {

    /** The profile of a run without a known demo name. */
    public static final String PROFILE = "catalogue";

    private static final Logger log = LoggerFactory.getLogger(Catalogue.class);

    public record Entry(String name, String chapter, String summary) {
    }

    private static final List<Entry> DEMOS = List.of(
            new Entry("spring-setup", "14",
                    "what Boot auto-configures, spring.kafka.* -> client configs, KafkaAdmin topics, which kafka-clients runs"),
            new Entry("spring-template", "15",
                    "KafkaTemplate: sync vs async send, ProducerListener, several templates from one factory, the chapter-02 matrix, Micrometer"),
            new Entry("spring-listener-acks", "16",
                    "@KafkaListener ack modes (ackMode attribute), commits per mode, nack, ConsumerSeekAware replay, filter + interceptor"),
            new Entry("spring-concurrency", "17",
                    "concurrency vs partitions, batch listeners, containers on virtual threads, asyncAcks hand-off, pause/resume"),
            new Entry("spring-error-handling", "18",
                    "DefaultErrorHandler back-off, dead-letter publishing with headers, poison pills, @RetryableTopic non-blocking retries"),
            new Entry("spring-transactions", "19",
                    "transaction-id-prefix, executeInTransaction, allow-non-transactional, container-managed exactly-once"),
            new Entry("spring-share", "20",
                    "share consumers: ShareAckMode EXPLICIT/MANUAL, release/reject/renew, recoverer, acquisition locks, concurrency"),
            new Entry("spring-serdes", "21",
                    "Jackson 3 JSON: __TypeId__ tokens, per-listener deserializer properties, message converter; Confluent Avro via a 2nd factory"));

    public static Optional<Entry> find(String name) {
        return DEMOS.stream().filter(e -> e.name().equals(name)).findFirst();
    }

    public static List<Entry> all() {
        return DEMOS;
    }

    /**
     * Lists the demos, in the same layout as {@code Run} in plain-clients; an unknown demo name is an error (exit code 2).
     *
     * @param runDemos {@code tweaks.demo.run=false} wires the profile without listing anything (DemoProfilesTest)
     */
    @Bean
    ApplicationRunner listDemos(@Value("${tweaks.demo.run:true}") boolean runDemos) {
        return arguments -> {
            if (!runDemos) {
                return;
            }
            List<String> plain = arguments.getNonOptionArgs();
            if (!plain.isEmpty()) {
                log.error("unknown demo: {}", plain.getFirst());
                DemoSupport.setExitCode(2);
            }
            var demos = new Table("demo", "ch.", "what it shows");
            DEMOS.forEach(e -> demos.row(e.name(), e.chapter(), e.summary()));
            log.info("usage: ./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments=\"<demo> [key=value ...]\"\n{}", demos);
            log.info("chapter 22 (testing) has no demo; it is the module's test suite: ./mvnw -q -pl spring-boot-kafka -am verify");
        };
    }
}
