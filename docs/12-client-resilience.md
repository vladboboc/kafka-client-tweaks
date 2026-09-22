# 12 · Client resilience and operations

**Demo:** `client-resilience` · [ClientResilienceDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ClientResilienceDemo.java)

## The problem

Brokers restart, leaders move, a whole rack goes dark, and a client keeps running for months. The
settings in this chapter decide what happens in those minutes: where reads come from, how long the
producer keeps trying, how fast it reconnects, and how the people running the cluster can see what the
client is doing.

## The knobs

| Config | Default | Meaning |
|---|---|---|
| `client.rack` (consumer) | none | lets the broker pick a replica in the same rack for fetches; needs broker `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector` and `broker.rack` on every broker |
| `retries` / `delivery.timeout.ms` (producer) | MAX / 120000 | chapter 03: the outage you can ride through without an error reaching the application |
| `retry.backoff.ms` / `retry.backoff.max.ms` | 100 / 1000 | exponential backoff between retries |
| `reconnect.backoff.ms` / `reconnect.backoff.max.ms` | 50 / 1000 | exponential backoff per broker connection attempt |
| `metadata.recovery.strategy` | `rebootstrap` | when *every* known broker is unreachable, re-resolve `bootstrap.servers` (KIP-899/1102). `none` = the old behaviour, spin on stale metadata forever |
| `metadata.recovery.rebootstrap.trigger.ms` | 300000 | also rebootstrap if no metadata could be obtained for this long |
| `connections.max.idle.ms` | 540000 | idle connections are closed |
| `socket.connection.setup.timeout.ms` / `.max.ms` | 10000 / 30000 | TCP connect budget |
| `default.api.timeout.ms` (consumer) | 60000 | budget for blocking calls like `commitSync()`, `position()` |
| `interceptor.classes` | none | `ProducerInterceptor` / `ConsumerInterceptor` implementations run inside the client on send/ack/poll/commit |
| `enable.metrics.push` | `true` | KIP-714: the client pushes its metrics to the brokers when an operator has created a subscription (`kafka-client-metrics.sh`) |
| `client.id` | random | tag on every metric, quota key, appears in broker logs. Always set it |

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="client-resilience"
```

Part 2 stops broker `kafka-2` for ~8 s. Follow the printed instructions or pass `broker-control=docker`.
Arguments: `rate=1500`, `seconds=24`.

## What you should see

**1. Follower fetching.** The topic's three partitions are led by brokers 1, 2 and 3 respectively. Bytes
received per broker connection while consuming all 6 000 records:

```
| consumer                         | bytes fetched from broker 1 | from broker 2 | from broker 3 |
| no client.rack (leader fetching) |                       75.5K |        101.9K |         62.8K |
| client.rack=rack-b               |                        1004 |        238.3K |          1004 |
```

**2. A broker dies mid-stream** (1 500 records/s, `kafka-2` stopped at 6 s and restarted at 14 s):

```
| t (s) | sent  | acked | failed | record-retry-total | record-error-total | event                |
|     5 |  7500 |  7488 |      0 |                  0 |                  0 |                      |
|     6 |  9001 |  9001 |      0 |                  0 |                  0 | docker stop kafka-2  |
|     7 | 10.5K |  9875 |      0 |                 19 |                  0 |                      |
|     8 | 12.0K | 12.0K |      0 |                 19 |                  0 |                      |
|   ... |       |       |        |                    |                    |                      |
|    14 | 21.0K | 21.0K |      0 |                 19 |                  0 | docker start kafka-2 |
|    15 | 22.5K | 22.5K |      0 |                 19 |                  0 |                      |
|   ... |       |       |        |                    |                    |                      |
| end   | 36.0K | 36.0K |      0 |                 19 |                  0 | flush()              |
```

Behind the table, the client log (WARN level) shows what the producer did in that second:
`NOT_LEADER_OR_FOLLOWER ... Going to request metadata update now`, a handful of `NOT_ENOUGH_REPLICAS`
retries while the ISR shrank, then silence.

Then the verification pass reads the topic back:

```
| sent  | acked | failed | in topic | distinct | duplicates |
| 36.0K | 36.0K |      0 |    36.0K |    36.0K |          0 |
```

**3. Interceptors.** 500 records stamped with a `sent-at` header by a `ProducerInterceptor`, measured by a
`ConsumerInterceptor`:

```
| producer onSend | producer onAcknowledgement | consumer records with header | avg produce->consume ms | consumer onCommit |
|             500 |                        500 |                          500 |                    1737 |                88 |
```

(The 1.7 s "latency" is the consumer starting after the producer finished; the interceptor measures
exactly what it is asked to.)

**4. Telemetry.** The producer asks for its client instance id (`clientInstanceId(Duration)`). The stack
has a subscription for all clients (`kafka-client-metrics --describe --name tweaks-all-clients`), but the
handshake runs in the background on the sender thread, and a client that has lived for half a second
usually prints `not negotiated yet (null)`. A long-running client gets a stable UUID that appears in broker
logs and metrics.

## Reading the numbers

- **Follower fetching moves bytes, not leadership.** With `client.rack=rack-b` every fetch went to
  broker 2 regardless of which broker leads the partition; without it, bytes came from each partition's
  leader. In a multi-AZ deployment this is the difference between paying cross-zone traffic for every
  consumer and paying it once for replication. The cost: a follower may be slightly behind the leader,
  so end-to-end latency can grow by a replication hop, and a consumer's lag metric is measured against the
  leader's end offset.
- **A broker dying mid-stream is a non-event for a well-configured producer.** The timeline shows a
  short dip in acknowledged records while leadership moves and metadata refreshes, `record-retry-total`
  climbing during the gap, zero `record-error-total`, and the verification finds every sequence number
  exactly once. Idempotence made the retries safe; `acks=all` + `min.insync.replicas=2` on RF=3 topics made
  them complete; the default `delivery.timeout.ms` gave them time.
- **Interceptors are the extension point for cross-cutting concerns**: tracing headers (this is where
  OpenTelemetry instruments Kafka clients), audit counters, schema policy checks, the produce-to-consume
  latency the demo measures from a header. They run on the client's threads: keep them fast and never
  block in `onSend`.
- **KIP-714 telemetry** inverts the monitoring direction: instead of scraping JMX from every
  application host, the broker pulls a subscription-defined set of metrics from every client. The client
  instance id ties metrics, logs and quotas to one process. Until an operator creates a subscription the
  client pushes nothing, so leaving `enable.metrics.push=true` costs one request at startup.
- **Rebootstrap** matters in clouds where the entire broker set can be replaced (node pools, IP changes).
  A client that started against three IPs that no longer exist recovers only if it goes back to
  `bootstrap.servers`, so put a DNS alias or a load balancer there, or list every broker.

## When to use what

| Situation | Setting |
|---|---|
| multi-AZ consumers, cross-zone traffic bills | `client.rack=<zone>` on consumers; `RackAwareReplicaSelector` + `broker.rack` on brokers |
| rolling broker restarts must be invisible | producer defaults (`delivery.timeout.ms=120000`, idempotence); consumers: nothing to do, they follow leadership |
| a fleet of thousands of clients | raise `reconnect.backoff.max.ms` and `retry.backoff.max.ms` so a broker returning is not stampeded |
| brokers get replaced, not restarted | `metadata.recovery.strategy=rebootstrap` (default) and a stable name in `bootstrap.servers` |
| distributed tracing | an interceptor pair (or the OpenTelemetry Kafka instrumentation, which is one) |
| central client monitoring | keep `enable.metrics.push=true`, create subscriptions on the cluster, plug a metrics reporter into the brokers |
| per-application quotas and log correlation | `client.id=<app>-<instance>` |
