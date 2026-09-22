package io.kafkatweaks.spring.parallel.recipe;

import io.kafkatweaks.spring.TopicsConfig;
import io.kafkatweaks.spring.parallel.ParallelProbe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Chapter 17 · More records at the same time: {@code concurrency} (consumers, capped by the partition count), a batch
 * listener (one call per poll), and one consumer with per-partition workers acknowledging out of order. Measured by the
 * {@code spring-concurrency} demo (docs/17-spring-concurrency-batch.md) on a 6-partition topic, 1 ms of work per record.
 * {@code probe.*} calls are the demo's simulated work and measurement: your processing goes there.
 */
@Component
@Profile("spring-concurrency")
public class ParallelListeners implements DisposableBean {

    private final ParallelProbe probe;
    private final Map<Integer, ExecutorService> partitionWorkers = new ConcurrentHashMap<>();

    public ParallelListeners(ParallelProbe probe) {
        this.probe = probe;
    }

    // ---- 1. the same handler, 1 / 3 / 6 / 8 consumers: concurrency = child containers = consumers in the group ----

    @KafkaListener(id = "par-1", groupId = "spring-par-1", clientIdPrefix = "par-1", topics = TopicsConfig.PARALLEL, concurrency = "1")
    public void one(ConsumerRecord<String, String> record) {
        probe.work();
        probe.done("par-1", 1);
    }

    @KafkaListener(id = "par-3", groupId = "spring-par-3", clientIdPrefix = "par-3", topics = TopicsConfig.PARALLEL, concurrency = "3")
    public void three(ConsumerRecord<String, String> record) {
        probe.work();
        probe.done("par-3", 1);
    }

    @KafkaListener(id = "par-6", groupId = "spring-par-6", clientIdPrefix = "par-6", topics = TopicsConfig.PARALLEL, concurrency = "6")
    public void six(ConsumerRecord<String, String> record) {
        probe.work();
        probe.done("par-6", 1);
    }

    /** More consumers than partitions: two of the eight sit idle. */
    @KafkaListener(id = "par-8", groupId = "spring-par-8", clientIdPrefix = "par-8", topics = TopicsConfig.PARALLEL, concurrency = "8")
    public void eight(ConsumerRecord<String, String> record) {
        probe.work();
        probe.done("par-8", 1);
    }

    // ---- 2. a batch listener: the whole poll per call; concurrency comes from spring.kafka.listener.concurrency ----

    @KafkaListener(id = "par-batch", groupId = "spring-par-batch", clientIdPrefix = "par-batch", topics = TopicsConfig.PARALLEL, batch = "true")
    public void batch(List<ConsumerRecord<String, String>> records) {
        probe.bulkWrite(records.size());   // the cost model of a bulk write: one round trip per CALL, not one per record
        probe.done("par-batch", records.size());
    }

    // ---- 3. one consumer thread, six virtual workers, acknowledgements out of order (asyncAcks) -----------------

    @KafkaListener(id = "par-async", groupId = "spring-par-async", clientIdPrefix = "par-async", topics = TopicsConfig.PARALLEL,
            concurrency = "1", ackMode = "MANUAL", containerFactory = "asyncAckContainerFactory",
            properties = "max.partition.fetch.bytes:16384")   // small per-partition fetches => a poll mixes all partitions (chapter 10)
    public void async(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // Chapter 10's per-partition workers: one virtual thread per partition keeps the order within the partition;
        // acknowledge() may be called from that thread because the container was told to expect out-of-order acks.
        partitionWorkers.computeIfAbsent(record.partition(),
                        p -> Executors.newSingleThreadExecutor(Thread.ofVirtual().name("worker-p" + p).factory()))
                .execute(() -> {
                    probe.work();
                    // Acknowledge FIRST, then count: done() is what the demo's await watches, so counting first
                    // lets it stop the container while this record's acknowledgement is still outstanding - and
                    // with asyncAcks the container is paused waiting for exactly that acknowledgement.
                    ack.acknowledge();
                    probe.done("par-async", 1);
                });
    }

    /** Also run when the context closes, so a demo that fails half way still leaves no worker behind. Idempotent. */
    @Override
    public void destroy() {
        shutdownWorkers();
    }

    public void shutdownWorkers() {
        partitionWorkers.values().forEach(ExecutorService::shutdown);
    }

    // ---- 4. paused and resumed from the outside (BackPressure) ---------------------------------------------------

    @KafkaListener(id = "par-pause", groupId = "spring-par-pause", clientIdPrefix = "par-pause", topics = TopicsConfig.PARALLEL, concurrency = "1")
    public void pausable(ConsumerRecord<String, String> record) {
        probe.work();
        probe.done("par-pause", 1);
    }
}
