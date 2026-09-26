package io.kafkatweaks.spring;

import io.kafkatweaks.common.Args;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
 * What every demo needs around its body: the header {@code Run} logs in plain-clients, the {@code key=value}
 * arguments, the exit code, and the listener containers. Auto-startup is off for all demos (application.yml),
 * so a demo seeds its topic first and then starts exactly the listeners it is about.
 */
@Component
public class DemoSupport {

    @FunctionalInterface
    public interface Body {
        void run(Args args) throws Exception;
    }

    private static final Logger log = LoggerFactory.getLogger(DemoSupport.class);
    private static final AtomicInteger EXIT_CODE = new AtomicInteger();

    private final KafkaProperties kafka;
    private final KafkaListenerEndpointRegistry registry;
    private final boolean runDemos;

    /** @param runDemos {@code tweaks.demo.run=false} wires a demo profile without running its body (DemoProfilesTest) */
    public DemoSupport(KafkaProperties kafka, KafkaListenerEndpointRegistry registry, @Value("${tweaks.demo.run:true}") boolean runDemos) {
        this.kafka = kafka;
        this.registry = registry;
        this.runDemos = runDemos;
    }

    public static int exitCode() {
        return EXIT_CODE.get();
    }

    /** For the {@link Catalogue} runner: a run that named an unknown demo exits with 2. */
    static void setExitCode(int code) {
        EXIT_CODE.set(code);
    }

    /** Wraps a demo body into the ApplicationRunner a chapter's {@code @Profile} configuration exposes as a bean. */
    public ApplicationRunner demo(String name, Body body) {
        if (!runDemos) {
            return applicationArguments -> { };
        }
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
            log.info("== {}  (chapter {})\n   {}\n   bootstrap.servers={}   kafka-clients={}   args={}",
                    name, entry.chapter(), entry.summary(), bootstrap, AppInfoParser.getVersion(), args);
            try {
                body.run(args);
            } catch (Exception e) {
                log.error("demo {} failed", name, e);
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

    /** {@code ProducerFactory.getConfigurationProperties()} as the Properties the shared {@code Knobs} tables expect. */
    public static Properties toProperties(Map<String, ?> configs) {
        var p = new Properties();
        p.putAll(configs);
        return p;
    }
}
