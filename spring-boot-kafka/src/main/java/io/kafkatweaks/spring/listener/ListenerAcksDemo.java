package io.kafkatweaks.spring.listener;

import io.kafkatweaks.common.MetricsReport;
import io.kafkatweaks.common.Seed;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.ClientCapture;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.listener.recipe.AckModeListeners;
import io.kafkatweaks.spring.listener.recipe.CountingRecordInterceptor;
import io.kafkatweaks.spring.listener.recipe.ListenerRecipe;
import io.kafkatweaks.spring.listener.recipe.ReplayListener;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Chapter 16: {@code @KafkaListener} and acknowledgment modes. Measures {@link AckModeListeners}, {@link ReplayListener}
 * and {@link ListenerRecipe}; everything else in this file is measurement.
 * <ol>
 *   <li>the same topic through five listeners that differ only in {@code ackMode}: commits per mode</li>
 *   <li>{@code Acknowledgment.nack(Duration)}: what is redelivered, what is discarded, how long the pause is</li>
 *   <li>{@link ReplayListener}: {@code ConsumerSeekAware} replays the last 100 records of every partition</li>
 *   <li>a {@code RecordFilterStrategy} picked by the {@code filter} attribute, and a global {@code RecordInterceptor}</li>
 * </ol>
 * <pre>
 *   records=6000   records the topic is seeded with (RECORD mode commits once per record, so keep it modest)
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-listener-acks")
public class ListenerAcksDemo {

    /** listener id -> what its ackMode means; the ids are also the {@code clientIdPrefix} values. */
    static final Map<String, String> ACK_MODES = Map.of(
            "acks-record", "RECORD: commit after every record",
            "acks-batch", "BATCH: commit after the records of a poll were processed",
            "acks-time", "TIME: like BATCH, but only if ack-time (1s) passed since the last commit",
            "acks-count", "COUNT: like BATCH, but only once ack-count (1000) records were processed",
            "acks-manual", "MANUAL_IMMEDIATE: commit when the listener calls acknowledge() (here: every 500th record)");
    static final List<String> ACK_MODE_LISTENERS = List.of("acks-record", "acks-batch", "acks-time", "acks-count", "acks-manual");

    @Bean
    ApplicationRunner springListenerAcks(DemoSupport support, ListenerProbe probe, CountingRecordInterceptor interceptor, ClientCapture capture) {
        return support.demo("spring-listener-acks", args -> {
            long records = args.getLong("records", 6000);
            String topic = TopicsConfig.LISTENER;
            long total;
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets;
            try (var topics = new Topics()) {
                total = Seed.ensure(topics, topic, 3, records, 200);
                endOffsets = topics.endOffsets(topic);
                // Every group starts from scratch on every run (auto-offset-reset=earliest does the rest).
                for (String group : List.of("spring-acks-record", "spring-acks-batch", "spring-acks-time", "spring-acks-count",
                        "spring-acks-manual", "spring-acks-nack", "spring-acks-replay", "spring-acks-filter")) {
                    topics.deleteGroup(group);
                }
            }

            // ---- 1. ack modes --------------------------------------------------------------------------------
            var modes = new Table("listener", "records", "commits", "commit-latency-avg ms", "start -> drained ms", "ackMode");
            for (String id : ACK_MODE_LISTENERS) {
                var sw = Stopwatch.start();
                support.start(id);   // includes the group join
                await(() -> probe.count(id), total, Duration.ofMinutes(3), id);
                double drainMs = sw.elapsedMillis();
                DemoSupport.sleep(1500);   // TIME commits on the first poll after ack-time; give it that poll before reading the counter
                double commits = capture.sumConsumerMetric(id, MetricsReport.CONSUMER_COORDINATOR, "commit-total");
                double latency = capture.sumConsumerMetric(id, MetricsReport.CONSUMER_COORDINATOR, "commit-latency-avg");
                support.stop(id);
                modes.row(id, probe.count(id), "%.0f".formatted(commits), Double.isNaN(latency) ? "-" : "%.2f".formatted(latency),
                        "%.0f".formatted(drainMs), ACK_MODES.get(id));
            }
            modes.print("1. %d records, 3 partitions, one listener per ackMode (commit-total from consumer-coordinator-metrics; commits are synchronous)".formatted(total));

            // ---- 2. nack -------------------------------------------------------------------------------------
            support.start("acks-nack");
            await(() -> probe.nackTimeline().size(), 12, Duration.ofSeconds(20), "acks-nack");
            support.stop("acks-nack");
            var timeline = new Table("t ms", "partition", "offset", "attempt", "listener did");
            probe.nackTimeline().stream().limit(12).forEach(d -> timeline.row(d.tMs(), d.partition(), d.offset(), d.attempt(), d.action()));
            timeline.print("2. ackMode=MANUAL, max.poll.records=5, partition 0 assigned manually from offset 0; the listener nacks offset 3 once");
            System.out.println("   nack: the offsets acknowledged so far are committed, the rest of the poll (offset 4) is dropped before the listener sees it,");
            System.out.println("   the consumer is paused for the sleep, then polling resumes AT the nacked record. Resolution = poll-timeout (1 s here).");

            // ---- 3. replay -----------------------------------------------------------------------------------
            support.start("acks-replay");
            await(probe::replayed, 300, Duration.ofSeconds(20), "acks-replay");
            DemoSupport.sleep(500);
            support.stop("acks-replay");
            var seeks = new Table("partition", "end offset", "first offset the listener saw", "records replayed");
            var firsts = probe.firstReplayedOffsets();
            endOffsets.entrySet().stream().sorted(Map.Entry.comparingByKey((a, b) -> Integer.compare(a.partition(), b.partition()))).forEach(e ->
                    seeks.row(e.getKey().partition(), e.getValue(), firsts.get(e.getKey().partition()), e.getValue() - firsts.getOrDefault(e.getKey().partition(), e.getValue())));
            seeks.print("3. ConsumerSeekAware.onPartitionsAssigned -> seekRelative(-100): %d records replayed, committed offsets ignored".formatted(probe.replayed()));

            // ---- 4. filter + interceptor -----------------------------------------------------------------------
            support.start("acks-filter");
            await(() -> interceptor.count("spring-acks-filter"), total, Duration.ofSeconds(60), "acks-filter");
            support.stop("acks-filter");
            var filter = new Table("where", "records", "what");
            filter.row("RecordInterceptor (global, before the listener)", interceptor.count("spring-acks-filter"), "saw every record of the poll");
            filter.row("RecordFilterStrategy oddOffsetFilter", interceptor.count("spring-acks-filter") - probe.count("acks-filter"), "discarded (odd offsets)");
            filter.row("listener method", probe.count("acks-filter"), "got the rest; offsets of discarded records are still committed with the batch");
            filter.print("4. filter and interceptor on the acks-filter listener");
            System.out.printf("   the interceptor also counted the other groups: %s%n", interceptor.counts());
        });
    }

    private static void await(LongSupplier value, long target, Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (value.getAsLong() < target) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(what + ": reached " + value.getAsLong() + " of " + target + " within " + timeout);
            }
            DemoSupport.sleep(50);
        }
    }
}
