package io.kafkatweaks.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Prints the knobs a chapter is about: the value the demo is running with next to the client default,
 * so the reader always knows what actually changed. Values are the parsed ones the client will use
 * (defaults applied), not the raw strings passed in.
 */
public final class Knobs {

    private Knobs() {
    }

    public static void printProducer(Properties effective, String... keys) {
        var baseline = Env.producer("defaults");
        print("producer knobs", new ProducerConfig(effective), new ProducerConfig(baseline), keys);
    }

    public static void printConsumer(Properties effective, String... keys) {
        var baseline = Env.consumer("defaults", "defaults");
        print("consumer knobs", new ConsumerConfig(effective), new ConsumerConfig(baseline), keys);
    }

    private static void print(String title, AbstractConfig effective, AbstractConfig defaults, String... keys) {
        var table = new Table("config", "this run", "client default");
        for (String key : keys) {
            Object v = value(effective, key);
            Object d = value(defaults, key);
            table.row(key, render(v), render(d) + (String.valueOf(v).equals(String.valueOf(d)) ? "" : "  <- changed"));
        }
        table.print(title);
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
