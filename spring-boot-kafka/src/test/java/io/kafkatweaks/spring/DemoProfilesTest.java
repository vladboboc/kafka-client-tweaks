package io.kafkatweaks.spring;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Every demo profile still wires up after the recipe beans moved around: the context refreshes (every
 * {@code containerFactory = "..."} and {@code filter = "..."} name resolves, no bean is ambiguous), the listener
 * containers are the expected ones, and the demo's runner exists. No broker: the {@code test} profile keeps KafkaAdmin
 * from creating topics, containers do not auto-start, and {@code tweaks.demo.run=false} turns the demo body into a no-op.
 */
class DemoProfilesTest {

    static Stream<Arguments> profiles() {
        return Stream.of(
                arguments("spring-setup", List.of()),
                arguments("spring-template", List.of()),
                arguments("spring-listener-acks", List.of("acks-record", "acks-batch", "acks-time", "acks-count", "acks-manual",
                        "acks-nack", "acks-filter", "acks-replay")),
                arguments("spring-concurrency", List.of("par-1", "par-3", "par-6", "par-8", "par-batch", "par-async", "par-pause")),
                arguments("spring-error-handling", List.of("errors-blocking", "errors-retryable", "errors-retryable-retry-1000",
                        "errors-retryable-retry-2000", "errors-retryable-retry-4000", "errors-retryable-dlt")),
                arguments("spring-transactions", List.of("txn-per-record", "txn-per-batch")),
                arguments("spring-share", List.of("share-explicit", "share-manual", "share-recover", "share-lock-2s", "share-lock-10s",
                        "share-lock-renew")),
                arguments("spring-serdes", List.of("serdes-json", "serdes-view", "serdes-mapped", "serdes-converter", "serdes-avro")));
    }

    @Test
    void everyDemoOfTheCatalogueIsCovered() {
        assertThat(profiles().map(a -> (String) a.get()[0]))
                .containsExactlyElementsOf(Catalogue.all().stream().map(Catalogue.Entry::name).toList());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("profiles")
    void theProfileWiresItsListenersAndItsRunner(String profile, List<String> listenerIds) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SpringTweaksApplication.class)
                .profiles(profile, "test")
                .properties("tweaks.demo.run=false")
                .run()) {
            assertThat(context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainerIds())
                    .containsExactlyInAnyOrderElementsOf(listenerIds);
            assertThat(context.getBeansOfType(ApplicationRunner.class)).isNotEmpty();
        }
    }
}
