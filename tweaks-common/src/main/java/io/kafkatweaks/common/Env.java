package io.kafkatweaks.common;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Where the cluster is. Resolution order: JVM system property, environment variable, docker-compose default.
 * <pre>
 *   -Dbootstrap.servers=host:port      or   BOOTSTRAP_SERVERS=host:port
 *   -Dschema.registry.url=http://...   or   SCHEMA_REGISTRY_URL=http://...
 * </pre>
 * The defaults match {@code docker-compose.yml}: the three brokers advertise their EXTERNAL listener on
 * localhost:19092/29092/39092. Listing all three matters for chapter 12 (rebootstrap) and for the
 * durability demos where one of them is stopped on purpose.
 */
public final class Env {

    public static final String DEFAULT_BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092";
    public static final String DEFAULT_SCHEMA_REGISTRY = "http://localhost:8081";

    private Env() {
    }

    public static String bootstrapServers() {
        return resolve("bootstrap.servers", "BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP);
    }

    public static String schemaRegistryUrl() {
        return resolve("schema.registry.url", "SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY);
    }

    /** Producer with String key/value serializers and nothing else tuned: every chapter starts from here. */
    public static Properties producer(String clientId) {
        var p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    /** Consumer with String deserializers, a group id, and {@code auto.offset.reset=earliest} so demos see what they produced. */
    public static Properties consumer(String groupId, String clientId) {
        var p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    public static Properties admin() {
        var p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        p.put(CommonClientConfigs.CLIENT_ID_CONFIG, "tweaks-admin");
        return p;
    }

    private static String resolve(String property, String envVar, String defaultValue) {
        String v = System.getProperty(property);
        if (v == null || v.isBlank()) {
            v = System.getenv(envVar);
        }
        return (v == null || v.isBlank()) ? defaultValue : v;
    }
}
