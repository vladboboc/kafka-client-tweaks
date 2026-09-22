package io.kafkatweaks.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/** Fills a topic for the consumer chapters, quickly (chapter 02 settings), only if it does not already hold enough. */
public final class Seed {

    private Seed() {
    }

    /** Ensures the topic holds at least {@code records} records of about {@code sizeBytes}; returns the total present afterwards. */
    public static long ensure(Topics topics, String topic, int partitions, long records, int sizeBytes) {
        // Big compressed batches: fastest way to fill a topic. Note that the batches a producer writes are the
        // batches a consumer fetches (the broker never re-batches), so demos that care about fetch sizes seed
        // with a producer configured like a typical application instead (see the overload below).
        return ensure(topics, topic, partitions, records, sizeBytes, Map.of(
                ProducerConfig.LINGER_MS_CONFIG, "50",
                ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(256 * 1024),
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
    }

    public static long ensure(Topics topics, String topic, int partitions, long records, int sizeBytes, Map<String, String> producerOverrides) {
        topics.ensure(topic, partitions);
        long present = topics.endOffsets(topic).values().stream().mapToLong(Long::longValue).sum();
        if (present >= records) {
            System.out.printf("topic %s already holds %d records%n", topic, present);
            return present;
        }
        var props = Env.producer("seed-" + topic);
        props.putAll(producerOverrides);
        long missing = records - present;
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (long i = 0; i < missing; i++) {
                producer.send(new ProducerRecord<>(topic, Payloads.key(i, 100), Payloads.json(present + i, sizeBytes)));
            }
            producer.flush();   // flush() does NOT surface per-record failures, so ask the log what really landed
        }
        // The callers size their poll loops and their "wait until N records arrived" conditions on this number:
        // returning the REQUESTED count after a partial seed turns a seeding failure into a hang or a timeout
        // somewhere else entirely.
        long now = topics.endOffsets(topic).values().stream().mapToLong(Long::longValue).sum();
        System.out.printf("seeded %d records into %s (now %d)%n", now - present, topic, now);
        if (now < records) {
            System.out.printf("WARNING: %d of the %d requested records were not written (check the broker log); "
                    + "the demo continues with %d%n", records - now, records, now);
        }
        return now;
    }
}
