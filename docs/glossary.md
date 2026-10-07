# Glossary

One or two lines per term, with the chapter that explains it properly. The [primer](primer.md) introduces the core
words in order. Config names are `in code font`; defaults are Kafka 4.3's.

## Kafka

| Term | Meaning | Explained in |
|---|---|---|
| **Accumulator** | The producer's in-memory buffer (`buffer.memory`, 32 MB), holding one open batch per partition until the sender thread ships it. | [01](01-producer-baseline.md), [02](02-producer-batching-compression.md) |
| **Acknowledgement (share groups)** | Per-record verdict a share consumer sends: `ACCEPT` (done), `RELEASE` (give it to someone again), `REJECT` (never deliver again). | [11](11-consumer-share-groups.md) |
| **Acquisition lock** | In a share group, the time a delivered record stays reserved for one consumer (`share.record.lock.duration.ms`); unacknowledged when it expires, the record is delivered again. | [11](11-consumer-share-groups.md) |
| **`acks`** | When the partition leader may answer a produce request: `0` never waits, `1` after its own write, `all` (default) after every in-sync replica has the batch. | [03](03-producer-durability.md) |
| **Assignor** | The algorithm that splits partitions among group members: `range`/`uniform` on the broker (consumer protocol), `RangeAssignor`, `CooperativeStickyAssignor`, … on the client (classic). | [09](09-consumer-rebalance.md) |
| **At-least-once / at-most-once / exactly-once** | What a crash can cost: records processed twice / records lost / neither. Decided by when you commit and whether processing is idempotent or transactional. | [primer](primer.md#10-delivery-guarantees), [08](08-consumer-offsets.md) |
| **`auto.offset.reset`** | Where a group starts on a partition it has no committed offset for: `latest` (default, only new records), `earliest`, or `none` (fail). | [08](08-consumer-offsets.md) |
| **Back-pressure** | Slowing the input when processing cannot keep up: a blocking `send()` on a full accumulator, or `pause()`/`resume()` of partitions on a consumer. | [02](02-producer-batching-compression.md), [10](10-consumer-parallel.md) |
| **Batch** | Records for one partition sent (and stored, and fetched) together; the unit of compression. Sized by `batch.size` (bytes, before compression). | [02](02-producer-batching-compression.md) |
| **Bootstrap servers** | The broker addresses a client starts from; it learns the rest of the cluster from them. | [primer](primer.md#1-brokers-and-the-cluster), [00](00-setup.md) |
| **Broker** | One Kafka server process; several form a cluster. This stack runs three. | [primer](primer.md#1-brokers-and-the-cluster) |
| **`client.id`** | A client's name; it tags every metric and appears in broker logs and quotas. | [01](01-producer-baseline.md) |
| **`client.rack` / follower fetching** | A consumer declaring its rack so the broker lets it read from a replica in the same rack instead of the leader. | [12](12-client-resilience.md) |
| **Commit (offsets)** | Storing a group's position per partition in `__consumer_offsets`, so the next owner of the partition resumes there. | [08](08-consumer-offsets.md) |
| **Compression** | `compression.type` (`lz4`, `snappy`, `zstd`, `gzip`) applied per batch by the producer; brokers store and consumers fetch it compressed. | [02](02-producer-batching-compression.md) |
| **Consume-transform-produce** | Read from one topic, write results to another, and commit the input offsets in the same transaction: Kafka's exactly-once pipeline. | [06](06-producer-transactions.md) |
| **Consumer group** | Consumers sharing a `group.id`; each partition is read by exactly one member at a time. | [primer](primer.md#8-consumer-groups-and-rebalancing), [09](09-consumer-rebalance.md) |
| **Coordinator** | The broker in charge of a group (group coordinator: membership, assignment, commits) or of a producer's transactions (transaction coordinator). | [09](09-consumer-rebalance.md), [06](06-producer-transactions.md) |
| **Delivery count** | How many times a share group has handed out a record; at `share.delivery.count.limit` the record is archived instead of redelivered. | [11](11-consumer-share-groups.md) |
| **`delivery.timeout.ms`** | Upper bound (120 s) on the time from `send()` to success or failure, retries included. | [03](03-producer-durability.md) |
| **Fencing / zombie** | A restarted producer with the same `transactional.id` bumps the producer epoch, so the old instance (the zombie) can no longer write or commit. | [06](06-producer-transactions.md) |
| **Fetch** | One consumer request to a broker; shaped by `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`. `poll()` returns records from fetches. | [07](07-consumer-fetch.md) |
| **Group protocol** | How a group rebalances: `classic` (client-side assignment, default in 4.3, deprecated) or `consumer` (KIP-848, broker-side and incremental). | [09](09-consumer-rebalance.md) |
| **Idempotence (producer)** | `enable.idempotence=true` (default): the broker drops duplicate batches caused by producer retries, and keeps their order. | [03](03-producer-durability.md) |
| **Idempotent handler** | Consumer code that can see a record twice without doing the work twice (e.g. skip an id already processed). | [08](08-consumer-offsets.md) |
| **Interceptor** | A `ProducerInterceptor`/`ConsumerInterceptor` class (`interceptor.classes`) that sees every record on the way in or out. | [12](12-client-resilience.md) |
| **ISR (in-sync replicas)** | The replicas of a partition that are caught up with the leader; only they can become leader, and `acks=all` waits for all of them. | [primer](primer.md#5-replicas-leaders-and-the-isr), [03](03-producer-durability.md) |
| **`isolation.level`** | `read_uncommitted` (default) or `read_committed`: whether a consumer sees records of open and aborted transactions. | [06](06-producer-transactions.md) |
| **KIP** | Kafka Improvement Proposal, the numbered design documents. The chapters cite KIP-848 (new consumer protocol), KIP-932 (share groups), KIP-714 (client metrics push), KIP-1274 (classic protocol deprecation). | [09](09-consumer-rebalance.md), [11](11-consumer-share-groups.md), [12](12-client-resilience.md) |
| **KRaft** | Kafka's built-in Raft quorum that stores cluster metadata; replaced ZooKeeper (removed in Kafka 4.0). | [primer](primer.md#1-brokers-and-the-cluster) |
| **Lag** | How many records a group is behind the end of a partition. | [primer](primer.md#8-consumer-groups-and-rebalancing), [07](07-consumer-fetch.md) |
| **Last stable offset (LSO)** | The offset before the first still-open transaction; a `read_committed` consumer cannot read past it. | [06](06-producer-transactions.md) |
| **Leader / follower** | The replica of a partition that takes writes (and, by default, reads) / the replicas that copy it. | [primer](primer.md#5-replicas-leaders-and-the-isr) |
| **`linger.ms`** | How long the producer waits for more records before sending a batch that is not full: 5 ms by default since 4.0. | [02](02-producer-batching-compression.md), [05](05-producer-low-latency.md) |
| **`max.in.flight.requests.per.connection`** | Unacknowledged produce requests per broker connection (5); with idempotence, order is kept up to 5. | [01](01-producer-baseline.md), [03](03-producer-durability.md) |
| **`max.poll.interval.ms`** | Longest allowed gap (5 min) between two `poll()` calls before the member is considered stuck and leaves the group. | [07](07-consumer-fetch.md) |
| **`min.insync.replicas`** | Topic/broker setting: the smallest ISR with which `acks=all` writes are still accepted (2 on this stack). | [03](03-producer-durability.md) |
| **Offset** | A record's sequence number within its partition. | [primer](primer.md#4-offsets) |
| **Partition** | One ordered, append-only log of a topic; the unit of parallelism, ordering and batching. | [primer](primer.md#3-partitions), [04](04-producer-partitioning.md) |
| **Partitioner** | Producer component that picks a record's partition: hash of the key, or sticky batching for key-less records. | [04](04-producer-partitioning.md) |
| **Poison pill** | A record that fails every time it is processed (bad format, bad data) and would block its partition forever if retried in place. | [08](08-consumer-offsets.md), [18](18-spring-error-handling-retry.md) |
| **Poll loop** | The consumer's `while (running) { poll(); process(); commit(); }`; everything a consumer does happens inside `poll()`. | [07](07-consumer-fetch.md) |
| **Rate metrics window** | Kafka's `*-rate` metrics average over ≥ 30 s, so short runs under-report; `*-total` counters are exact. | [01](01-producer-baseline.md) |
| **Rebalance** | Moving partitions between group members when one joins, leaves or dies. Eager (everyone stops) or incremental (only moving partitions pause). | [09](09-consumer-rebalance.md) |
| **Rebootstrap** | `metadata.recovery.strategy=rebootstrap` (default): when every known broker is gone, the client starts over from `bootstrap.servers`. | [12](12-client-resilience.md) |
| **Record** | One message: key, value, timestamp, headers. | [primer](primer.md#2-topics-and-records) |
| **Replication factor (RF)** | How many brokers hold a copy of each partition (3 here). | [primer](primer.md#5-replicas-leaders-and-the-isr) |
| **Retention** | How long (or how much) a topic keeps records; reading does not delete them. | [primer](primer.md#2-topics-and-records) |
| **Schema Registry / subject** | Service storing versioned schemas; a subject (by default `<topic>-value`) is the name a schema's versions are registered under. | [13](13-avro-schema-registry.md) |
| **Seek** | Moving a consumer's position on a partition: to an offset, a timestamp, the beginning or the end. | [08](08-consumer-offsets.md) |
| **Session timeout / heartbeat** | How a group notices a dead member: no heartbeat within `session.timeout.ms` (45 s). Broker settings under the consumer protocol. | [09](09-consumer-rebalance.md) |
| **Share group** | Queue semantics on a topic (KIP-932): records, not partitions, are handed to members, each acknowledged individually. | [11](11-consumer-share-groups.md) |
| **Static membership** | `group.instance.id`: a member that restarts within the session timeout gets its partitions back without a rebalance. | [09](09-consumer-rebalance.md) |
| **Sticky partitioner** | Default for key-less records: fill a batch for one partition, then switch, so batches get big. | [04](04-producer-partitioning.md) |
| **Topic** | A named stream of records, split into partitions. | [primer](primer.md#2-topics-and-records) |
| **Transaction / `transactional.id`** | Atomic writes to several partitions (plus input offsets); the stable id identifies the producer across restarts and fences zombies. | [06](06-producer-transactions.md) |
| **Watermark commit** | Committing, per partition, only the offset below which every record is done: needed when records of one partition finish out of order. | [10](10-consumer-parallel.md) |
| **Wire format (Confluent)** | Magic byte `0` + 4-byte schema id + the Avro binary: what the Avro serializer writes. | [13](13-avro-schema-registry.md) |

## Spring for Apache Kafka

| Term | Meaning | Explained in |
|---|---|---|
| **`@KafkaListener`** | A method Spring calls with records; Spring owns the consumer, the poll loop and the commits (the listener container). | [16](16-spring-listeners-acks.md) |
| **`@RetryableTopic`** | Non-blocking retries: a failed record goes to retry topics with delays and finally a DLT, so the main partition keeps flowing. | [18](18-spring-error-handling-retry.md) |
| **`AckMode`** | When the container commits: `RECORD`, `BATCH` (default), `TIME`, `COUNT`, `MANUAL`, `MANUAL_IMMEDIATE`. | [16](16-spring-listeners-acks.md) |
| **Batch listener** | A listener receiving a whole poll's records as a `List` (`batch = "true"`). | [17](17-spring-concurrency-batch.md) |
| **Concurrency** | Number of consumers (threads) a listener container runs; useful up to the partition count. | [17](17-spring-concurrency-batch.md) |
| **DLT (dead-letter topic)** | Where records that failed every retry are published, with the failure in headers (`DeadLetterPublishingRecoverer`). | [18](18-spring-error-handling-retry.md) |
| **`DefaultErrorHandler`** | The container's error handler: retries a failed record with a back-off, then hands it to a recoverer (e.g. the DLT). | [18](18-spring-error-handling-retry.md) |
| **`ErrorHandlingDeserializer`** | Wraps a deserializer so a record that cannot be deserialized reaches the error handler instead of failing every poll. | [18](18-spring-error-handling-retry.md) |
| **Embedded Kafka** | `@EmbeddedKafka`: a KRaft broker inside the test JVM. | [22](22-spring-testing.md) |
| **`KafkaAdmin` / `NewTopic` beans** | Boot's admin client; it creates the `NewTopic` beans of the application at startup. | [14](14-spring-boot-setup.md) |
| **`KafkaTemplate`** | Spring's producer wrapper; `send()` returns a `CompletableFuture`. | [15](15-spring-kafkatemplate.md) |
| **`nack()`** | `Acknowledgment.nack(Duration)` (MANUAL ack modes): commit what was acknowledged, seek back to this record, pause, then redeliver it and the rest of the poll. | [16](16-spring-listeners-acks.md) |
| **`ProducerListener`** | Callback bean for every `KafkaTemplate` send result. | [15](15-spring-kafkatemplate.md) |
| **`ShareAckMode`** | How a share container acknowledges: `EXPLICIT` (default: ACCEPT on return, a recoverer decides on exceptions), `MANUAL` (the listener calls `acknowledge()`, `release()` or `reject()`), `IMPLICIT`. | [20](20-spring-share-consumers.md) |
| **`spring.kafka.*`** | Boot's properties for the clients; typed keys for common settings, `properties[...]` for the rest. | [14](14-spring-boot-setup.md) |
| **`transaction-id-prefix`** | Setting it makes Boot's producer factory transactional and turns on Kafka transactions in the containers. | [19](19-spring-transactions.md) |
| **`__TypeId__`** | Header Spring's JSON serializer writes so the deserializer knows which class to create; mapped to tokens for loose coupling. | [21](21-spring-serialization.md) |
