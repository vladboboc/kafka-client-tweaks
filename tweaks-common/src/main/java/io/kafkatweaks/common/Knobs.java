package io.kafkatweaks.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Logs the knobs a chapter is about: the value the demo is running with next to the client default,
 * so the reader always knows what actually changed. Values are the parsed ones the client will use
 * (defaults applied), not the raw strings passed in.
 */
public final class Knobs {

    private static final Logger log = LoggerFactory.getLogger(Knobs.class);

    private Knobs() {
    }

    public static void logProducer(Properties effective, String... keys) {
        var baseline = Env.producer("defaults");
        logKnobs("producer knobs", new ProducerConfig(effective), new ProducerConfig(baseline), keys);
    }

    public static void logConsumer(Properties effective, String... keys) {
        var baseline = Env.consumer("defaults", "defaults");
        logKnobs("consumer knobs", new ConsumerConfig(effective), new ConsumerConfig(baseline), keys);
    }

    private static void logKnobs(String title, AbstractConfig effective, AbstractConfig defaults, String... keys) {
        var table = new Table("config", "this run", "client default");
        for (String key : keys) {
            Object v = value(effective, key);
            Object d = value(defaults, key);
            table.row(key, render(v), render(d) + (String.valueOf(v).equals(String.valueOf(d)) ? "" : "  <- changed"));
        }
        log.info("{}\n{}", title, table);
    }

    private static Object value(AbstractConfig cfg, String key) {
        Map<String, ?> values = cfg.values();
        if (values.containsKey(key)) {
            return values.get(key);
        }
        // Not a known config of this client (e.g. a serializer-specific setting): show the raw original.
        return cfg.originals().get(key);
    }

    private static String render(Object v) {
        return switch (v) {
            case null -> "null";
            case List<?> l -> String.join(",", l.stream().map(String::valueOf).toList());
            case Class<?> c -> c.getSimpleName();
            default -> String.valueOf(v);
        };
    }
}
