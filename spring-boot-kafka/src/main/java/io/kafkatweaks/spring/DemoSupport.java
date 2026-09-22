package io.kafkatweaks.spring;

import io.kafkatweaks.common.Args;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What every demo needs around its body: the header {@code Run} prints in plain-clients, the {@code key=value}
 * arguments, the exit code, and the listener containers. Auto-startup is off for all demos (application.yml),
 * so a demo seeds its topic first and then starts exactly the listeners it is about.
 */
@Component
public class DemoSupport {

    @FunctionalInterface
    public interface Body {
        void run(Args args) throws Exception;
    }

    private static final AtomicInteger EXIT_CODE = new AtomicInteger();

    private final KafkaProperties kafka;
    private final KafkaListenerEndpointRegistry registry;

    public DemoSupport(KafkaProperties kafka, KafkaListenerEndpointRegistry registry) {
        this.kafka = kafka;
        this.registry = registry;
    }

    public static int exitCode() {
        return EXIT_CODE.get();
    }

    /** Wraps a demo body into the ApplicationRunner a chapter's {@code @Profile} configuration exposes as a bean. */
    public ApplicationRunner demo(String name, Body body) {
        return applicationArguments -> {
            Catalogue.Entry entry = Catalogue.find(name).orElseThrow();
            // Non-option args only: "--spring.kafka.x=y" is a Spring property, not a demo knob.
            Args args = Args.parse(applicationArguments.getNonOptionArgs().toArray(String[]::new), 1);
            String bootstrap = String.join(",", kafka.getBootstrapServers());
            // The shared helpers (Env, Topics, Seed, the plain verification consumers) read these system properties,
            // so they talk to the same cluster Spring does, also when --spring.kafka.bootstrap-servers=... is given.
            System.setProperty("bootstrap.servers", bootstrap);
            String schemaRegistry = kafka.getProperties().get("schema.registry.url");
            if (schemaRegistry != null) {
                System.setProperty("schema.registry.url", schemaRegistry);
            }
            System.out.printf("== %s  (chapter %s)%n", name, entry.chapter());
            System.out.printf("   %s%n", entry.summary());
            System.out.printf("   bootstrap.servers=%s   kafka-clients=%s   args=%s%n%n", bootstrap, AppInfoParser.getVersion(), args);
            try {
                body.run(args);
            } catch (Exception e) {
                System.err.println("demo failed: " + e);
                e.printStackTrace();
                EXIT_CODE.set(1);
            }
        };
    }

    public KafkaProperties kafkaProperties() {
        return kafka;
    }

    public KafkaListenerEndpointRegistry registry() {
        return registry;
    }

    public MessageListenerContainer container(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container == null) {
            throw new IllegalArgumentException("no @KafkaListener with id " + listenerId + "; known: " + registry.getListenerContainerIds());
        }
        return container;
    }

    /** Starts the listeners with these ids and waits until each one has partitions assigned. */
    public void start(String... listenerIds) {
        for (String id : listenerIds) {
            container(id).start();
        }
        for (String id : listenerIds) {
            awaitAssignment(id, Duration.ofSeconds(30));
        }
    }

    public void stop(String... listenerIds) {
        for (String id : listenerIds) {
            container(id).stop();
        }
    }

    public void awaitAssignment(String listenerId, Duration timeout) {
        MessageListenerContainer container = container(listenerId);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Collection<TopicPartition> assigned = container.getAssignedPartitions();
            if (assigned != null && !assigned.isEmpty()) {
                return;
            }
            sleep(100);
        }
        throw new IllegalStateException("listener " + listenerId + " got no partitions within " + timeout);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** {@code ProducerFactory.getConfigurationProperties()} as the Properties the shared {@code Knobs} printer expects. */
    public static Properties toProperties(Map<String, ?> configs) {
        var p = new Properties();
        p.putAll(configs);
        return p;
    }
}
