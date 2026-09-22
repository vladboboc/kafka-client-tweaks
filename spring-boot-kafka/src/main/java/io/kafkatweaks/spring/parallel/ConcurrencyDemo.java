package io.kafkatweaks.spring.parallel;

import io.kafkatweaks.common.Seed;
import io.kafkatweaks.common.Stopwatch;
import io.kafkatweaks.common.Table;
import io.kafkatweaks.common.Topics;
import io.kafkatweaks.spring.DemoSupport;
import io.kafkatweaks.spring.TopicsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Chapter 17: concurrency, batch listeners and back-pressure.
 * <ol>
 *   <li>the same 1 ms handler with concurrency 1, 3, 6 and 8 on a 6-partition topic</li>
 *   <li>a batch listener: what one call receives, and the cost model that makes it worth it</li>
 *   <li>one consumer thread, six virtual workers, out-of-order acknowledgements ({@code asyncAcks})</li>
 *   <li>pause() / resume() from outside the listener</li>
 * </ol>
 * <pre>
 *   records=18000   records the topic is seeded with
 *   work=1          milliseconds of simulated work per record
 * </pre>
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-concurrency")
public class ConcurrencyDemo {

    /**
     * A second container factory for a container-level setting: Boot's configurer applies everything
     * {@code spring.kafka.listener.*} says (auto-startup, poll timeout, the virtual-thread executor ...), then the
     * factory is changed where the default one cannot be. Listeners pick it with {@code containerFactory = "..."}.
     * (asyncAcks happens to have a Boot key, spring.kafka.listener.async-acks; deliveryAttemptHeader, pauseImmediate
     * or micrometerTags do not, and this is how you set those.)
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> asyncAckContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        // Acknowledgements may arrive in any order; the container commits contiguous prefixes and pauses the
        // consumer until every record of the poll is acknowledged. Requires ackMode MANUAL or MANUAL_IMMEDIATE.
        factory.getContainerProperties().setAsyncAcks(true);
        return factory;
    }

    @Bean
    ApplicationRunner springConcurrency(DemoSupport support, ParallelListeners listeners, ContainerEvents events, MeterRegistry registry) {
        return support.demo("spring-concurrency", args -> {
            long records = args.getLong("records", 18_000);
            long workMs = args.getLong("work", 1);
            listeners.workMs(workMs);
            String topic = TopicsConfig.PARALLEL;
            long total;
            try (var topics = new Topics()) {
                // Fresh topic, seeded with PRODUCER DEFAULTS (16 KB batches, no compression): the broker never re-batches, and part 3
                // needs small batches so that a fetch capped at 16 KB per partition really returns a slice of every partition.
                topics.recreate(topic, 6);
                total = Seed.ensure(topics, topic, 6, records, 300, Map.of());
                for (String group : List.of("spring-par-1", "spring-par-3", "spring-par-6", "spring-par-8", "spring-par-batch", "spring-par-async", "spring-par-pause")) {
                    topics.deleteGroup(group);
                }
            }

            // ---- 1. scaling ----------------------------------------------------------------------------------
            var scaling = new Table("listener", "concurrency", "consumers that got partitions", "records", "start -> drained ms", "records/s", "ms per record", "timer mean ms", "threads");
            double singleMs = 0;
            for (String id : ParallelListeners.SCALING) {
                var sw = Stopwatch.start();
                support.start(id);
                await(() -> listeners.count(id), total, Duration.ofMinutes(3), id);
                double ms = sw.elapsedMillis();
                var container = (ConcurrentMessageListenerContainer<?, ?>) support.container(id);
                int children = container.getContainers().size();
                long active = container.getContainers().stream().filter(c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty()).count();
                double timerMean = timerMeanMs(registry, id);   // the timers are removed from the registry when the consumer closes
                support.stop(id);   // the child containers are discarded on stop, hence the counts above
                if (singleMs == 0) {
                    singleMs = ms;
                }
                scaling.row(id, container.getConcurrency(), active + " of " + children, listeners.count(id), "%.0f".formatted(ms),
                        "%.0f".formatted(listeners.count(id) / ms * 1000), "%.2f".formatted(ms / listeners.count(id)), "%.3f".formatted(timerMean),
                        listeners.threads(id).size() + (listeners.virtualThreads() ? " virtual" : " platform"));
            }
            scaling.print("1. %d records x %d ms of work each (parkNanos) on 6 partitions; concurrency = child containers = consumers in the group".formatted(total, workMs));
            System.out.printf("   effective work per record on this machine: %.2f ms (par-1 has one consumer, so its ms/record is the handler's cost + poll overhead)%n", singleMs / total);

            // ---- 2. batch listener -----------------------------------------------------------------------------
            var sw = Stopwatch.start();
            support.start("par-batch");
            await(() -> listeners.count("par-batch"), total, Duration.ofMinutes(3), "par-batch");
            double batchMs = sw.elapsedMillis();
            var batchContainer = (ConcurrentMessageListenerContainer<?, ?>) support.container("par-batch");
            double batchTimerMean = timerMeanMs(registry, "par-batch");
            support.stop("par-batch");
            var batch = new Table("listener", "concurrency (from yml)", "calls", "records/call avg", "largest call", "start -> drained ms", "records/s", "timer mean ms per call");
            batch.row("par-batch", batchContainer.getConcurrency(), listeners.batchCalls(), "%.1f".formatted((double) listeners.count("par-batch") / listeners.batchCalls()),
                    listeners.largestBatch(), "%.0f".formatted(batchMs), "%.0f".formatted(listeners.count("par-batch") / batchMs * 1000), "%.3f".formatted(batchTimerMean));
            batch.print("2. batch=\"true\": the listener gets List<ConsumerRecord> (<= max.poll.records=500), here at 2 ms per CALL instead of %d ms per record".formatted(workMs));

            // ---- 3. asyncAcks ------------------------------------------------------------------------------------
            sw = Stopwatch.start();
            support.start("par-async");
            await(() -> listeners.count("par-async"), total, Duration.ofMinutes(3), "par-async");
            double asyncMs = sw.elapsedMillis();
            support.stop("par-async");
            listeners.shutdownWorkers();
            var async = new Table("listener", "consumer threads", "workers", "records", "start -> drained ms", "records/s", "compare with");
            async.row("par-async", 1, "6 virtual, one per partition", listeners.count("par-async"), "%.0f".formatted(asyncMs),
                    "%.0f".formatted(listeners.count("par-async") / asyncMs * 1000), "par-1 (same single consumer) and par-6 (six consumers)");
            async.print("3. ackMode=MANUAL + asyncAcks: the poll thread hands records to per-partition workers and returns; workers acknowledge out of order");
            System.out.println("   max.partition.fetch.bytes=16K on this listener so that a poll mixes all six partitions (chapter 10's lesson: a poll");
            System.out.println("   otherwise returns one partition at a time and only one worker would be busy). The container pauses the consumer");
            System.out.printf("   until every record of a poll is acknowledged: %d pause/resume events came from par-async alone.%n",
                    events.pauseResume("par-async").size());

            // ---- 4. pause / resume -------------------------------------------------------------------------------
            MessageListenerContainer pausable = support.container("par-pause");
            support.start("par-pause");
            DemoSupport.sleep(400);
            pausable.pause();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!pausable.isContainerPaused() && System.nanoTime() < deadline) {
                DemoSupport.sleep(20);
            }
            long atPause = listeners.count("par-pause");
            DemoSupport.sleep(2000);
            long whilePaused = listeners.count("par-pause");
            pausable.resume();
            DemoSupport.sleep(1500);
            long afterResume = listeners.count("par-pause");
            support.stop("par-pause");
            var pause = new Table("moment", "records processed", "what happened");
            pause.row("pause() called after 400 ms", atPause, "takes effect before the next poll(); records already fetched are still delivered");
            pause.row("2 s later", whilePaused, "paused: poll() keeps the membership alive and returns nothing (" + (whilePaused - atPause) + " more records)");
            pause.row("1.5 s after resume()", afterResume, "delivery continues from the committed position");
            pause.print("4. MessageListenerContainer.pause() / resume() (spring-kafka's back-pressure switch; pauseImmediate=true stops after the current record)");
            System.out.printf("   events from the par-pause container: %s%n", events.pauseResume("par-pause"));
            System.out.printf("   ListenerContainerIdleEvents so far: %d (idle-event-interval=2s, from every idle container of this run)%n", events.idleEvents());
        });
    }

    /** Mean of the spring.kafka.listener timers of a listener (one per child container; the name tag carries the listener id). */
    private static double timerMeanMs(MeterRegistry registry, String listenerId) {
        double totalNanos = 0;
        long count = 0;
        for (Timer timer : registry.find("spring.kafka.listener").timers()) {
            String name = timer.getId().getTag("name");
            if (name != null && (name.equals(listenerId) || name.startsWith(listenerId + "-") || name.contains(listenerId))) {
                totalNanos += timer.totalTime(TimeUnit.NANOSECONDS);
                count += timer.count();
            }
        }
        return count == 0 ? Double.NaN : totalNanos / count / 1_000_000;
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
