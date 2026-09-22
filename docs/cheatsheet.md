# Cheat sheet · which knob for which goal

Client defaults are Kafka 4.3 (`kafka-clients` 4.3.x). Broker/topic settings are marked **(broker)**.

## Producer

| Goal | Turn | Watch (`producer-metrics`) | Costs | Chapter |
|---|---|---|---|---|
| more records/s, MB/s | `batch.size` ↑ (64–512 KB), `linger.ms` ↑ (10–100), `compression.type=zstd` | `batch-size-avg`, `records-per-request-avg`, `compression-rate-avg` | latency = linger + bigger requests; CPU for compression | 02 |
| lowest latency | `linger.ms=0`, `compression.type=none`, async `send()` with callback | `record-queue-time-avg`, `request-latency-avg` | fewer records per request | 05 |
| no acknowledged record ever lost | `acks=all` (default), **(topic)** `min.insync.replicas=2`, RF=3, `enable.idempotence=true` (default) | `record-retry-total`, `record-error-total` | replication round trip in every ack | 03 |
| ride through a broker restart silently | `delivery.timeout.ms` ≥ expected outage (default 120 s); keep `retries` at default | `record-retry-total` | records sit in the buffer meanwhile | 03, 12 |
| fail fast instead | `delivery.timeout.ms` small (≥ `linger.ms + request.timeout.ms`), handle the callback exception | `record-error-total` | | 03 |
| ordering per key | key the record; idempotence on (keeps order up to 5 in-flight) | | hot keys = hot partitions | 04 |
| max batching for key-less events | leave the sticky partitioner (default); avoid `RoundRobinPartitioner` | `batch-size-avg` | | 04 |
| a key without ordering | `partitioner.ignore.keys=true` or put the id in the value | | per-key order gone | 04 |
| `send()` blocking / `BufferExhaustedException` | you are faster than the cluster: compress, batch, add partitions/brokers; `buffer.memory` only delays it | `bufferpool-wait-ratio`, `buffer-available-bytes` | | 02 |
| atomic multi-partition writes, exactly-once Kafka→Kafka | `transactional.id`, `sendOffsetsToTransaction`, consumer `isolation.level=read_committed` | | commit cost: batch by poll or by time, never per record | 06 |
| read from the local rack | `client.rack` **(broker: `replica.selector.class=RackAwareReplicaSelector`)** | `consumer-node-metrics` per node | possible extra replication lag on reads | 12 |

## Consumer

| Goal | Turn | Watch | Costs | Chapter |
|---|---|---|---|---|
| more records per poll | `max.poll.records` ↑ | `records-per-request-avg`, `fetch-size-avg` | each poll takes longer: mind `max.poll.interval.ms` | 07 |
| fewer, fuller fetches at low traffic | `fetch.min.bytes` ↑ + `fetch.max.wait.ms` | `fetch-latency-avg`, `fetch-rate` | up to `fetch.max.wait.ms` extra latency | 07 |
| big records / many partitions per consumer | `max.partition.fetch.bytes`, `fetch.max.bytes` | `fetch-size-avg` | memory = partitions × `max.partition.fetch.bytes` | 07 |
| slow handler keeps getting kicked | `max.poll.records` ↓ or `max.poll.interval.ms` ↑, or hand work to threads and keep polling | `CommitFailedException`, `time-between-poll-avg` | | 07, 10 |
| never process a record twice | at-most-once: commit **before** processing (records can be lost) | | | 08 |
| never lose a record | at-least-once: `enable.auto.commit=false`, commit **after** processing; make the handler idempotent | `commit-latency-avg` | duplicates on crash | 08 |
| replay / rewind | `seek`, `seekToBeginning`, `offsetsForTimes`; `kafka-consumer-groups --reset-offsets` | | | 08 |
| where a new group starts | `auto.offset.reset=earliest|latest|none` | | `none` fails loudly | 08 |
| smooth scaling in/out | `group.protocol=consumer` (KIP-848; classic is deprecated) | `rebalance-latency-avg`, `rebalance-total` | server-side session settings | 09 |
| restarts without rebalances | `group.instance.id` (static membership) | | dead member holds partitions until session timeout | 09 |
| more parallelism than partitions | per-partition workers (ordered) or per-record tasks (unordered) inside one consumer; pause/resume for back-pressure; commit watermarks | `records-lag-max` | complexity; ordering only per partition | 10 |
| queue semantics, per-record retry and dead-letter | share group: `KafkaShareConsumer`, `share.acknowledgement.mode=explicit`, ACCEPT/RELEASE/REJECT; **(group)** `share.record.lock.duration.ms`, `share.delivery.count.limit` | | no ordering across members, no replay by offset | 11 |
| survive "all brokers changed" | `metadata.recovery.strategy=rebootstrap` (default), list all brokers or a DNS alias in `bootstrap.servers` | | | 12 |

## Serialization

| Goal | Turn | Chapter |
|---|---|---|
| compact wire format with a contract | Confluent Avro + Schema Registry; `auto.register.schemas=false` + `use.latest.version=true` in production | 13 |
| several event types on one topic | `value.subject.name.strategy=RecordNameStrategy` / `TopicRecordNameStrategy` | 13 |
| safe evolution | `BACKWARD` (default): add fields with defaults; consumers upgrade first. `FULL` when both sides deploy independently | 13 |
| `SecurityException: Forbidden …` on deserialize | Avro ≥ 1.12.1 class allow-list: register generated classes (`AvroTrust`) or `-Dorg.apache.avro.SERIALIZABLE_PACKAGES` | 13 |

## Spring Boot (part 2): the same knobs as `spring.kafka.*`

Typed keys convert for you (`batch-size: 64KB`, `fetch-max-wait: 250ms`); everything else goes in
`spring.kafka.<producer|consumer|admin>.properties["[key]"]`; one listener can override with
`@KafkaListener(properties = "key:value")`. A free-form entry wins over the typed key for the same config. Chapter 14
has the full table.

| Plain config | Spring | Chapter |
|---|---|---|
| `bootstrap.servers`, `client.id` | `spring.kafka.bootstrap-servers`, `spring.kafka.client-id` (containers append `-0`, `-1`, …) | 14 |
| `acks`, `batch.size`, `buffer.memory`, `compression.type`, `retries` | `spring.kafka.producer.acks` / `batch-size` / `buffer-memory` / `compression-type` / `retries` | 14, 15 |
| `linger.ms`, `enable.idempotence`, `delivery.timeout.ms`, … (no typed key) | `spring.kafka.producer.properties["[linger.ms]"]` | 14 |
| `KafkaProducer` | `KafkaTemplate` (`send()` → `CompletableFuture<SendResult>`, `ProducerListener`, several templates from one factory) | 15 |
| `transactional.id`, `initTransactions`/`commitTransaction` | `spring.kafka.producer.transaction-id-prefix` → `KafkaTransactionManager`, `executeInTransaction`, `@Transactional`, container-managed EOS | 19 |
| `key/value.serializer`, `key/value.deserializer` | `spring.kafka.producer.value-serializer`, `spring.kafka.consumer.value-deserializer`; `JacksonJsonSerializer` + `spring.json.type.mapping` / `trusted.packages` | 21 |
| `group.id` | `spring.kafka.consumer.group-id`, or `@KafkaListener(groupId)` | 16 |
| `auto.offset.reset`, `isolation.level`, `max.poll.records`, `max.poll.interval.ms`, `fetch.min.bytes`, `fetch.max.wait.ms`, `heartbeat.interval.ms` | `spring.kafka.consumer.auto-offset-reset` / `isolation-level` / `max-poll-records` / `max-poll-interval` / `fetch-min-size` / `fetch-max-wait` / `heartbeat-interval` | 14 |
| `enable.auto.commit`, `commitSync`/`commitAsync` | the container commits: `spring.kafka.listener.ack-mode` (RECORD, BATCH, TIME, COUNT, MANUAL, …) or `@KafkaListener(ackMode)` + `Acknowledgment.acknowledge()`/`nack()` | 16 |
| `group.protocol`, `group.instance.id`, `partition.assignment.strategy`, `client.rack`, `max.partition.fetch.bytes` | `spring.kafka.consumer.properties["[…]"]` | 14 |
| consumers per group, workers per consumer | `spring.kafka.listener.concurrency` / `@KafkaListener(concurrency)`; `type: batch` or `batch = "true"` for `List<ConsumerRecord>`; `asyncAcks` for out-of-order acks; `pause()`/`resume()` | 17 |
| retry loop, dead-letter topic | `DefaultErrorHandler` + `BackOff`, `DeadLetterPublishingRecoverer` (`<topic>-dlt`, `kafka_dlt-*` headers), `ErrorHandlingDeserializer`, `@RetryableTopic` + `@DltHandler` | 18 |
| `KafkaShareConsumer`, `acknowledge(record, type)` | `ShareKafkaListenerContainerFactory` (no Boot auto-config), `ShareAckMode` EXPLICIT / MANUAL, `ShareAcknowledgment`, `ShareConsumerRecordRecoverer`; group configs still via `Admin` | 20 |
| `schema.registry.url`, `specific.avro.reader` | `spring.kafka.properties["[schema.registry.url]"]`, `spring.kafka.consumer.properties["[specific.avro.reader]"]`; a second factory when formats differ per topic | 21 |
| `MockProducer`, a local broker | `MockProducerFactory`, `@EmbeddedKafka` (KRaft), `KafkaTestUtils`; `Binder` to test the property mapping itself | 22 |

## Broker/topic settings the client chapters depend on

| Setting | This stack | Why |
|---|---|---|
| `default.replication.factor` / `min.insync.replicas` | 3 / 2 | `acks=all` means something |
| `auto.create.topics.enable` | false | typos fail instead of creating 1-partition topics |
| `group.consumer.session.timeout.ms` / `group.consumer.heartbeat.interval.ms` | 45000 / 5000 | KIP-848 session settings live on the broker |
| `replica.selector.class` | `RackAwareReplicaSelector` | enables `client.rack` follower fetching |
| `group.share.min.record.lock.duration.ms` | 2000 | lets the share-group demo use 2 s locks (production floor is 15 s) |
| `share.version` feature | 1 | share groups enabled (Kafka ≥ 4.2 formats new clusters this way) |
