package io.kafkatweaks.spring;

import java.util.List;
import java.util.Optional;

/** The demos of part 2 in chapter order. The name is also the Spring profile and the {@code application-<name>.yml} file. */
public final class Catalogue {

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

    private Catalogue() {
    }

    public static Optional<Entry> find(String name) {
        return DEMOS.stream().filter(e -> e.name().equals(name)).findFirst();
    }

    public static List<Entry> all() {
        return DEMOS;
    }

    /** Same layout as {@code Run.printList()} in plain-clients. */
    public static void print() {
        System.out.println("usage: ./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments=\"<demo> [key=value ...]\"\n");
        System.out.printf("%-24s %-4s %s%n", "demo", "ch.", "what it shows");
        DEMOS.forEach(e -> System.out.printf("%-24s %-4s %s%n", e.name(), e.chapter(), e.summary()));
        System.out.println("\nchapter 22 (testing) has no demo; it is the module's test suite: ./mvnw -q -pl spring-boot-kafka -am verify");
    }
}
