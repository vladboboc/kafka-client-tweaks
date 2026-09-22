# Kafka Client Tweaks

A hands-on guide to tuning Apache Kafka **producers and consumers** for what you actually need:
throughput, latency, durability, ordering, exactly-once, queue semantics. Every chapter is a short
write-up plus a runnable demo that prints the client metrics the tweak affects, so you see the effect
instead of taking it on faith.

Part 1 (`plain-clients`, chapters 01–13) uses the plain `org.apache.kafka:kafka-clients` library, because
that is what every framework configures underneath. Part 2 (`spring-boot-kafka`, chapters 14–22) maps the same
knobs onto **Spring Boot 4.1 / spring-kafka 4.1**: `spring.kafka.*`, `KafkaTemplate`, `@KafkaListener`
acknowledgement modes, concurrency, error handlers and `@RetryableTopic`, transactions, share consumers,
JSON and Avro serializers, and testing on an embedded KRaft broker.

Stack: **Java 25 · Maven Wrapper · Docker Compose · Confluent Platform 8.3.2 (Apache Kafka 4.3, KRaft) · kafka-clients 4.3.1 · Spring Boot 4.1.1 / spring-kafka 4.1.1 · Schema Registry · Kafbat UI**.

---

## Quick start

Docker and a JDK 25+ are the only prerequisites (`./mvnw` downloads Maven itself).

```bash
docker compose up -d --wait
```

```bash
./mvnw -q verify
```

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-baseline"
```

```bash
./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="spring-setup"
```

Then open the UI at http://localhost:8080 and read on from [docs/00-setup.md](docs/00-setup.md).

| Endpoint | URL |
|---|---|
| Kafka brokers (from the host) | `localhost:19092`, `localhost:29092`, `localhost:39092` |
| Kafbat UI (topics, groups, lag, messages, schemas) | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |

`docker compose down -v` removes everything, data volumes included.

---

## Chapters

Plain demos: `./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="<demo> [key=value ...]"`.
Spring demos: `./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="<demo> [key=value ...]"`.
No arguments lists them. In part 1 keys containing a dot (`linger.ms=20`) are passed to the Kafka client as-is; in
part 2 the same override is a Spring property (`--spring.kafka.producer.properties.linger.ms=20`).

| # | Chapter | Demo | What you tune |
|---|---|---|---|
| 00 | [Setup and how to run](docs/00-setup.md) | – | the stack, the command shape, the shared helpers |
| **Producer** | | | |
| 01 | [Anatomy, defaults, metrics](docs/01-producer-baseline.md) | `producer-baseline` | the send path, sync vs async, 4.x defaults, reading `metrics()` |
| 02 | [Throughput: batching and compression](docs/02-producer-batching-compression.md) | `producer-batching` | `batch.size`, `linger.ms`, `compression.type` + levels, `buffer.memory`, `max.block.ms` |
| 03 | [Durability, ordering, retries](docs/03-producer-durability.md) | `producer-durability` | `acks`, `min.insync.replicas`, idempotence, the timeout chain, a broker stopped mid-demo |
| 04 | [Partitioning and keys](docs/04-producer-partitioning.md) | `producer-partitioning` | sticky vs round-robin, key hashing, hot keys, `partitioner.ignore.keys`, custom `Partitioner` |
| 05 | [Latency first](docs/05-producer-low-latency.md) | `producer-low-latency` | `linger.ms=0`, `acks`, no compression; sequential and paced workloads |
| 06 | [Transactions and exactly-once](docs/06-producer-transactions.md) | `producer-transactions` | `transactional.id`, `read_committed`, commit cost, consume-transform-produce, zombie fencing |
| **Consumer** | | | |
| 07 | [The poll loop and fetch tuning](docs/07-consumer-fetch.md) | `consumer-fetch` | `max.poll.records`, `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`, `max.poll.interval.ms` |
| 08 | [Offsets and delivery guarantees](docs/08-consumer-offsets.md) | `consumer-offsets` | commit before/after, idempotent handlers, auto-commit timing, `auto.offset.reset`, seeking |
| 09 | [Group protocol and rebalancing](docs/09-consumer-rebalance.md) | `consumer-rebalance` | `group.protocol=consumer` (KIP-848) vs `classic`, assignors, static membership; a live timeline |
| 10 | [Scaling and parallelism](docs/10-consumer-parallel.md) | `consumer-parallel` | partitions vs consumers, per-partition workers on virtual threads, pause/resume, watermark commits |
| 11 | [Queues for Kafka: share groups](docs/11-consumer-share-groups.md) | `consumer-share` | `KafkaShareConsumer`, explicit acks (ACCEPT/RELEASE/REJECT), acquisition locks, delivery limits |
| 12 | [Client resilience and operations](docs/12-client-resilience.md) | `client-resilience` | `client.rack`, a broker dying mid-stream, interceptors, KIP-714 telemetry, rebootstrap |
| **Serialization** | | | |
| 13 | [Schema Registry and Avro](docs/13-avro-schema-registry.md) | `avro-roundtrip` | generated records, wire size vs JSON, `auto.register.schemas`, subject strategies, evolution |
| **Spring Boot** | | | |
| 14 | [Spring Boot wiring and the `spring.kafka.*` mapping](docs/14-spring-boot-setup.md) | `spring-setup` | what Boot auto-configures, typed keys vs `properties[...]`, `KafkaAdmin` topics, a 4.2 client on 4.3 brokers |
| 15 | [KafkaTemplate](docs/15-spring-kafkatemplate.md) | `spring-template` | sync vs async `send()`, `ProducerListener`, several templates from one factory, the chapter-02 matrix, Micrometer |
| 16 | [`@KafkaListener` and acknowledgment modes](docs/16-spring-listeners-acks.md) | `spring-listener-acks` | `AckMode` per listener (`ackMode` attribute), commits per mode, `nack()`, `ConsumerSeekAware`, filter and interceptor |
| 17 | [Concurrency, batch listeners and back-pressure](docs/17-spring-concurrency-batch.md) | `spring-concurrency` | concurrency vs partitions, batch listeners, containers on virtual threads, `asyncAcks`, pause/resume |
| 18 | [Error handling, retries, dead letters and `@RetryableTopic`](docs/18-spring-error-handling-retry.md) | `spring-error-handling` | `DefaultErrorHandler` back-off, `DeadLetterPublishingRecoverer`, poison pills, non-blocking retry topics |
| 19 | [Transactions in Spring](docs/19-spring-transactions.md) | `spring-transactions` | `transaction-id-prefix`, `executeInTransaction`, `@Transactional`, container-managed exactly-once |
| 20 | [Share consumers (queues) in Spring](docs/20-spring-share-consumers.md) | `spring-share` | `ShareAckMode` EXPLICIT/MANUAL, `release()`/`reject()`/`renew()`, recoverer, acquisition locks |
| 21 | [Serialization in Spring: JSON and Avro](docs/21-spring-serialization.md) | `spring-serdes` | Jackson 3 `__TypeId__` tokens, per-listener deserializer properties, message converter, Avro via a second factory |
| 22 | [Testing Spring Kafka applications](docs/22-spring-testing.md) | (test suite) | `Binder`-checked property mapping, `MockProducerFactory`, embedded KRaft broker, dead letters and share listeners in tests |
| | [Cheat sheet](docs/cheatsheet.md) | | goal → knob → metric → cost, on one page; plain config → `spring.kafka.*` |

---

## Version matrix

| Component | Version | Note |
|---|---|---|
| Java | **25** | records, pattern matching, virtual threads (chapters 10, 17); built with `--release 25`, so JDK 26 works too |
| Apache Kafka clients (plain modules) | **4.3.1** | same line as the brokers; `linger.ms` default 5, KIP-848 consumer protocol, share groups, `DefaultPartitioner` removed |
| Spring Boot | **4.1.1** | its BOM decides every version of the Spring module: Spring Framework 7.0.9, spring-kafka 4.1.1, Jackson 3.1.5, Micrometer 1.17.1, JUnit 6.0.3 |
| spring-kafka | **4.1.1** | `@KafkaListener(ackMode)`, share consumer containers, Jackson 3 serializers, KRaft-only embedded broker |
| Apache Kafka clients (Spring module) | **4.2.1** | Boot-managed: what spring-kafka 4.1.1 is compiled and tested against; a 4.2 client on 4.3 brokers is supported (chapter 14 shows how to override) |
| Confluent Platform | **8.3.2** | `cp-kafka` and `cp-schema-registry` images; Apache Kafka 4.3 inside; KRaft only (ZooKeeper left in CP 8.0) |
| Confluent serializers | 8.3.2 | `kafka-avro-serializer` + Schema Registry client, from `packages.confluent.io` |
| Apache Avro | 1.12.2 | `avro-maven-plugin` generates the `Order` class; ≥ 1.12.1 class allow-list handled by `AvroTrust` |
| Kafbat UI | 1.5.0 | open source Kafka UI, one container |
| Maven / wrapper | 3.9.16 / 3.3.4 | `only-script` wrapper: no jar in the repo, Maven downloaded on first use |
| JUnit / AssertJ | 6.1.3 / 3.27.7 in the plain modules, Boot-managed in the Spring module | unit tests for the helpers; embedded-broker tests in chapter 22 |

Why these move together: **CP 8.3 = Kafka 4.3 = kafka-clients 4.3 = Confluent serializers 8.3**. Mixing
lines works (Kafka is wire-compatible), but the behaviour documented here is 4.3's; the Spring module deliberately
runs the 4.2.1 client Boot ships, see the notes below.

---

## Layout

```
├── docker-compose.yml        3 KRaft brokers (racks a/b/c) + Schema Registry + Kafbat UI + topic-init
├── docker/kafka/             create-topics.sh: the tweaks.* topics, share.version check
├── docs/                     one markdown file per chapter + cheatsheet
├── tweaks-common/            Maven module with the helpers both demo modules share
│   ├── src/main/avro/        Order.avsc, compiled by avro-maven-plugin
│   └── src/main/java/io/kafkatweaks/
│       ├── common/           Env, Knobs, MetricsReport, Table, Topics, Payloads, Order, JsonSerde, Seed, Workload
│       └── avro/             AvroTrust (the Avro >= 1.12.1 class allow-list)
├── plain-clients/            Maven module with the plain-client demos (part 1)
│   └── src/main/java/io/kafkatweaks/
│       ├── Run.java          the dispatcher: one demo name per chapter
│       ├── producer/         chapters 01–06
│       ├── consumer/         chapters 07–12
│       └── avro/             chapter 13
└── spring-boot-kafka/        Spring Boot module (part 2); the demo name is the Spring profile
    ├── src/main/java/io/kafkatweaks/spring/
    │   ├── SpringTweaksApplication, Catalogue, DemoSupport, ClientCapture, TopicsConfig
    │   ├── setup/ template/ listener/ parallel/ errors/ txn/ share/ serdes/     chapters 14–21
    ├── src/main/resources/   application.yml + application-<demo>.yml: that chapter's spring.kafka.* knobs
    └── src/test/java/        chapter 22: property mapping, mock producer, embedded KRaft broker tests
```

---

## Notes and trade-offs

**Three brokers on a laptop.** Half of the producer chapters are about what `acks=all`,
`min.insync.replicas` and idempotence buy you, and that needs replicas to stop. Broker heaps are capped at
512 MB and every container has a memory limit; the whole stack settles around 3 GB. Stopping **one** broker
is safe (the KRaft quorum needs 2 of 3); stopping two takes the cluster down.

**Numbers are relative.** The demos run against Docker Desktop on a laptop; absolute figures will differ
on your machine and mean nothing for production sizing. The *shape* of every comparison is the lesson.
Presets in one demo run sequentially, so later presets enjoy a warm JIT; each demo warms up first, and
`runs=` lets you reorder.

**Rate metrics lie in short runs.** Kafka's `*-rate` metrics divide by a window of at least 30 s
(`metrics.sample.window.ms × (metrics.num.samples − 1)`), so a 3 s run under-reports by 10×. The demos
therefore print their own elapsed-time throughput next to the metrics, and `*-total` counters where exact
counts matter.

**Kafka client logging is at WARN.** Config dumps and coordinator chatter would bury the tables. The
warnings you do see (deprecated `classic` protocol, `NOT_ENOUGH_REPLICAS` retries, a poll timeout) are part of
the lesson. `-Dkafka.log=INFO` (plain) or `KAFKA_LOG=INFO` (Spring) turns everything on.

**`kafka-clients` 4.3 still defaults `group.protocol` to `classic`.** KIP-1274 deprecates it and the client
logs a warning; the KIP-848 `consumer` protocol is opt-in (`group.protocol=consumer`) until a later release
flips the default. Chapter 09 runs both.

**Two kafka-clients versions, on purpose.** The plain modules use 4.3.1, the brokers' line. The Spring module
takes 4.2.1 from Boot's BOM, because that is what spring-kafka 4.1.1 is compiled and tested against; a 4.2
client against 4.3 brokers is a supported combination, and everything in chapters 14–22 works with it. Forcing
4.3.1 there is one `dependencyManagement` entry in the module's POM (chapter 14 shows it); we chose not to, so
that the Spring chapters describe what a Boot 4.1 application really runs.

**Boot's BOM versus the root POM.** The root `pom.xml` manages nothing but Avro (Boot's BOM does not know Avro);
the plain modules take their versions from the root's `*.version` properties, the Spring module imports
`spring-boot-dependencies`. A version managed in a parent POM would silently override an imported BOM, so
keeping the root's `dependencyManagement` empty is what keeps the Spring module on the versions Boot tested.

**Listeners do not start on their own in the Spring demos.** `spring.kafka.listener.auto-startup=false` in
`application.yml`: containers would otherwise start during the context refresh, before the demo has seeded its
topic. Each demo starts its listeners through the `KafkaListenerEndpointRegistry`, and the tests do the same.

**Slim images.** The CP 8.3 images have no `curl`/`awk`; the compose healthchecks and the topic-init script
use bash built-ins. If you copy the stack elsewhere, keep that in mind.
