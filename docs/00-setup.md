# 00 · Setup and how to run a chapter

New to Kafka? Read the [primer](primer.md) alongside this page: it explains partitions, offsets, replicas and consumer
groups on the cluster started below. The [docs index](README.md) suggests a reading order for your level.

## What you need

| Tool | Version | Why |
|---|---|---|
| Docker (Desktop or Engine) with Compose v2 | any recent | the Kafka cluster, Schema Registry and the UI run in containers |
| JDK | **25 or newer** | the demos are compiled with `--release 25`; JDK 26 works too |
| Maven | none | `./mvnw` (Maven Wrapper, *only-script* flavour) downloads Maven 3.9.16 on first use |

Roughly 3 GB of RAM for the containers: three brokers capped at 512 MB heap each, Schema Registry, the UI.

## Start the stack

```bash
docker compose up -d --wait
```

`--wait` returns when every healthcheck passes and `topic-init` has exited 0. Then:

```bash
docker compose ps
```

| Container | What | Where from the host |
|---|---|---|
| `kafka-1`, `kafka-2`, `kafka-3` | Confluent Platform 8.3.2 brokers (Apache Kafka 4.3), KRaft combined mode, one "rack" each | `localhost:19092`, `localhost:29092`, `localhost:39092` |
| `topic-init` | creates the `tweaks.*` topics with deliberate partition counts, checks share groups are enabled, exits | – |
| `schema-registry` | Confluent Schema Registry 8.3.2 | http://localhost:8081 |
| `kafka-ui` | [Kafbat UI](https://github.com/kafbat/kafka-ui): topics, consumer groups, lag, messages, schemas | http://localhost:8080 |

Cluster-wide defaults that the chapters lean on (see the comments in [docker-compose.yml](../docker-compose.yml)):
`default.replication.factor=3`, `min.insync.replicas=2`, `auto.create.topics.enable=false`.

## Run a demo

The short way is the `demo` script at the repository root: give it the chapter number, then any arguments.

```bash
./demo 02 records=50000 payload=random
```

On Windows run `.\demo.cmd 02 records=50000 payload=random` from PowerShell, or `demo.cmd …` from `cmd.exe`.
Chapters 01–13 go to the plain module, 14–21 to the Spring module, and 22 runs the Spring test suite. `./demo plain`
and `./demo spring` list the demos. The script prints the Maven command it runs, which is the long form below.

Every demo is started the same way. Without arguments you get the list:

```bash
./mvnw -q -pl plain-clients -am compile exec:java
```

With a demo name, optionally followed by `key=value` arguments:

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="producer-batching records=50000 payload=random"
```

Two kinds of arguments:

- **demo parameters** have no dot: `records=`, `size=`, `runs=`, `wait=` … each chapter documents its own;
- **anything with a dot is a Kafka client property** and is applied on top of the demo's own configuration:
  `linger.ms=20`, `fetch.min.bytes=1048576`, `group.protocol=classic`.

Point the demos at another cluster with `-Dbootstrap.servers=host:port` (or the `BOOTSTRAP_SERVERS` env var)
and `-Dschema.registry.url=…` (`SCHEMA_REGISTRY_URL`).

Everything a demo reports is logged through SLF4J and Logback, the pair Spring Boot uses by default:
[logback.xml](../plain-clients/src/main/resources/logback.xml) configures the plain module, `logging.*` in
[application.yml](../spring-boot-kafka/src/main/resources/application.yml) the Spring one, with the same pattern. Each
message starts with the time, the level and the class that logged it; a table is one multi-line message, so its rows
stay aligned under that first line:

```
14:06:06.883 INFO  Workload - throughput & end-to-end ack latency
| run      | records | payload MB | elapsed ms | records/s | MB/s | ack p50 ms | ack p99 ms | ack max ms | errors |
|----------|---------|------------|------------|-----------|------|------------|------------|------------|--------|
| baseline |   20.0K |      10.91 |       2547 |      7852 | 4.29 |       1322 |       2128 |       2141 |      0 |
```

The chapters' "What you should see" excerpts leave the time / level / logger prefix out. Kafka's own client logging
is kept at WARN so the tables stay readable; `-Dkafka.log=INFO` turns it up.

`./mvnw` resolves to `mvnw.cmd` automatically in both `cmd.exe` and PowerShell, so no path change is needed on
Windows. PowerShell has one specific catch, though: it splits a `-D` flag at the first `.` unless the *whole*
flag is quoted as a single token, so `-Dexec.args="producer-batching"` (quoting only the value, as in the
examples above) fails with `Unknown lifecycle phase`. Quote the whole thing instead:
`"-Dexec.args=producer-batching"`; the same applies to `-Dspring-boot.run.arguments=` below. `cmd.exe` and
Git Bash run the examples above unmodified.

## Run a Spring Boot demo (part 2)

The Spring module ([chapters 14–22](14-spring-boot-setup.md)) has its own dispatcher with the same shape. Without
arguments it lists the demos:

```bash
./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run
```

The first argument is the demo name (it is also the Spring profile, so `application-<demo>.yml` holds that chapter's
`spring.kafka.*` knobs); `key=value` demo parameters follow as in part 1:

```bash
./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="spring-template records=20000"
```

Kafka client properties are Spring properties here. An override anywhere in the arguments beats the chapter's YAML:

```bash
./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="spring-template --spring.kafka.producer.properties.linger.ms=50"
```

`BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` work as in part 1; `KAFKA_LOG=INFO` turns the client logging up
(`-Dkafka.log` is the plain module's flag). After `./mvnw -q package` the module is also a runnable jar:
`java -jar spring-boot-kafka/target/spring-boot-kafka-1.0-SNAPSHOT.jar spring-setup`.

## Build and tests

```bash
./mvnw -q verify
```

Compiles everything, generates the Avro classes and runs the tests of all three modules: the broker-free unit
tests of `tweaks-common` and `plain-clients`, and the Spring module's suite ([chapter 22](22-spring-testing.md)),
which starts a one-node KRaft broker inside the JVM for a few of its tests; no Docker needed, about 25 s. If
`JAVA_HOME` is not set and `java` is not on the `PATH`, point the wrapper at your JDK: `JAVA_HOME=/path/to/jdk-25 ./mvnw …`.

## Reset

```bash
docker compose down -v
```

Stops everything and deletes the data volumes, so the next `up` starts from empty topics.

## What the demos have in common

Each plain demo lives in `plain-clients/src/main/java/io/kafkatweaks/<producer|consumer|avro>/` and is
registered by name in [`Run.java`](../plain-clients/src/main/java/io/kafkatweaks/Run.java); each Spring demo is a
`@Profile`-bound configuration under `spring-boot-kafka/src/main/java/io/kafkatweaks/spring/<chapter>/`, listed in
[`Catalogue.java`](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/Catalogue.java).

**The tweak itself is in the `recipe` subpackage next to each demo** (`producer/recipe/`, `consumer/recipe/`,
`avro/recipe/`, `spring/<chapter>/recipe/`): small classes with only the production code, one comment per setting.
The demo calls its recipe and measures it; [recipes.md](recipes.md) lists every recipe with what it bought. Plain
recipes use none of the helpers below, so they can be copied as they are.

The demos share the helpers of the `tweaks-common` module (package `io.kafkatweaks.common`):

| Helper | Purpose |
|---|---|
| `Env` | where the cluster is; base `Properties` for producer / consumer / admin |
| `Knobs` | logs the configs a chapter is about: value for this run next to the client default |
| `Workload` | the one workload every producer chapter reuses, so only the configuration differs between runs |
| `MetricsReport` | reads `producer.metrics()` / `consumer.metrics()` and logs the ones that matter |
| `Topics` | AdminClient chores: create topics with a chosen partition count, reset a group, show assignments, lag, ISR |
| `Seed` | fills a topic for the consumer chapters, only if it does not already hold enough records |
| `Payloads`, `Order`, `JsonSerde` | test data: compressible JSON, incompressible random text, a small domain record |

---

← [Primer · Kafka in 20 minutes](primer.md) · [Index](README.md) · [01 · Producer anatomy, defaults and metrics](01-producer-baseline.md) →
