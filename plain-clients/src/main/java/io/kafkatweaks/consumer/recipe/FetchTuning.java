package io.kafkatweaks.consumer.recipe;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Map;

/**
 * Chapter 07 · The poll loop and fetch tuning: fetch big, and give the handler batches it can finish in time.
 * <p>
 * Two machines feed a consumer. The background fetcher asks brokers for data in requests shaped by {@code fetch.*}
 * and {@code max.partition.fetch.bytes}; {@code poll()} hands your handler a slice of what it buffered, at most
 * {@code max.poll.records}. Measured by the {@code consumer-fetch} demo (docs/07-consumer-fetch.md), catching up on
 * 150 000 records of 512 bytes:
 * <pre>
 *   client defaults                  92.0K records/s    987 KB per fetch   2.75 fetches/s
 *   max.partition.fetch.bytes=64K    53.2K records/s     64 KB per fetch  40.67 fetches/s
 *   catchUp()                       120.4K records/s   3.54 MB per fetch   0.80 fetches/s
 * </pre>
 * The broker never re-batches: a fetch returns the batches the producer wrote, so producer batching (chapter 02)
 * decides how many records a fetch carries, too.
 */
public final class FetchTuning {

    private FetchTuning() {
    }

    /** Catch up on a backlog: few, big fetches and big polls. Memory: up to partitions x 4 MB buffered per consumer. */
    public static Map<String, Object> catchUp() {
        return Map.of(
                // Records per poll(), default 500. Only the size of the handler's batch: 50 or 500 made no difference
                // to throughput, because the fetcher runs ahead of poll() either way.
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000,
                // Data per partition per fetch response, default 1 MB. This is what moves throughput: 64 KB halved it.
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4 * 1024 * 1024,
                // The broker holds the fetch until this much data is available, default 1 byte ...
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024,
                // ... but never longer than this, default 500 ms.
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    }

    /**
     * A low-volume stream: one fetch per {@code maxWait} with everything that arrived, instead of one fetch per record,
     * at the cost of up to {@code maxWait} of extra latency. Measured tailing ~200 records/s: the defaults made 35 fetches/s
     * of 1 record each; {@code fewerFetches(64 * 1024, Duration.ofSeconds(2))} made 0.33 fetches/s of 94 records each.
     */
    public static Map<String, Object> fewerFetches(int minBytes, Duration maxWait) {
        return Map.of(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes,
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int) maxWait.toMillis());
    }

    /**
     * Keep a slow handler inside the poll deadline. {@code poll()} must be called again within
     * {@code max.poll.interval.ms} (default 5 min) or the consumer leaves the group, its partitions move to another
     * member, and its next commit fails with {@code CommitFailedException}. Choose {@code maxPollRecords} so that
     * {@code maxPollRecords x worst time per record} fits the interval with room to spare. Measured with 10 ms per
     * record and a 3 s interval: 500 records per poll was kicked out, 100 was fine.
     * <p>
     * And whatever the loop looks like: every {@code poll()} result counts. A loop that calls {@code poll()} only to
     * "wait for the assignment" and drops what it returned moves the position past those records, and the next commit
     * declares them processed.
     */
    public static Map<String, Object> pollBudget(int maxPollRecords, Duration maxPollInterval) {
        return Map.of(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) maxPollInterval.toMillis());
    }
}
