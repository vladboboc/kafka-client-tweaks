# 09 · Group protocol and rebalancing

**Demo:** `consumer-rebalance` · [ConsumerRebalanceDemo.java](../plain-clients/src/main/java/io/kafkatweaks/consumer/ConsumerRebalanceDemo.java)

## The problem

A consumer group is a set of processes sharing a topic's partitions. Every time a member joins, leaves
or dies, partitions move. How they move decides whether the whole group stops for a second (or ten)
on every deploy, or whether only the affected partitions pause. Kafka 4.x has two mechanisms for this,
and the client still defaults to the older one.

## Two protocols

| | `group.protocol=classic` (default in 4.3, **deprecated** by KIP-1274) | `group.protocol=consumer` (KIP-848, GA since 4.0) |
|---|---|---|
| who computes the assignment | the group leader (a consumer) using `partition.assignment.strategy` | the group coordinator (broker) using `group.remote.assignor` (`uniform` default, `range`) |
| how members learn of changes | JoinGroup / SyncGroup round trips; everyone rejoins | through heartbeats; incremental, per member |
| eager vs incremental | eager with `RangeAssignor`/`RoundRobinAssignor` (revoke everything, then reassign); incremental with `CooperativeStickyAssignor` | always incremental |
| `session.timeout.ms` / `heartbeat.interval.ms` | client settings (45000 / 3000) | **broker** settings: `group.consumer.session.timeout.ms` (45000), `group.consumer.heartbeat.interval.ms` (5000); client values ignored |
| `max.poll.interval.ms` | client, 300000, same meaning in both: your thread must call `poll()` in time | |
| static membership `group.instance.id` | supported | supported |

Other knobs: `ConsumerRebalanceListener` (`onPartitionsRevoked` is where you commit before losing a
partition), `group.consumer.assignors` and `group.consumer.migration.policy` on the broker.

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="consumer-rebalance"
```

Arguments: `scenarios=consumer-uniform,classic-range` (subset), `settle=4`.

The same choreography runs per scenario on a 6-partition topic: A joins, B joins, C joins, B leaves
(and, in the static scenario, B comes back). Every revoke/assign callback is stamped on a timeline.

## What you should see

**Eager, `classic` + `RangeAssignor`.** Every change revokes everything from everyone:

```
 0.10s  A assigned [0,1,2,3,4,5]
 4.16s  --- B joins
 6.08s  A revoked  [0,1,2,3,4,5]        <- A gives up ALL six
 6.09s  B assigned [3,4,5]
 6.09s  A assigned [0,1,2]
10.11s  --- C joins
12.09s  A revoked  [0,1,2]              <- and again
12.09s  B revoked  [3,4,5]
12.09s  B assigned [2,3]
12.09s  C assigned [4,5]
12.09s  A assigned [0,1]
16.15s  --- B leaves
18.09s  A revoked  [0,1]                <- and again
18.09s  C revoked  [4,5]
18.10s  A assigned [0,1,2]
18.10s  C assigned [3,4,5]
```

**Incremental, `consumer` protocol (KIP-848), uniform assignor.** Only the moving partitions are touched:

```
 0.29s  A assigned [0,1,2,3,4,5]
 4.37s  --- B joins
 5.59s  A revoked  [3,4,5]              <- A keeps 0,1,2 and keeps consuming them
 9.51s  B assigned [3,4,5]
13.57s  --- C joins
14.67s  B revoked  [5]
15.86s  A revoked  [2]
18.70s  C assigned [2,5]
22.73s  --- B leaves
22.73s  B revoked  [3,4]
23.81s  C assigned [3]
26.01s  A assigned [4]
```

`classic` + `CooperativeStickyAssignor` produces the same shape with two rebalance rounds per change.

**Static membership** (`consumer` protocol + `group.instance.id`): B closes and comes back 2 s later.

```
22.35s  --- B leaves (close, static member: no LeaveGroup)
22.35s  B revoked  [3,4]
24.75s  --- B restarts with the same group.instance.id
24.78s  B assigned [3,4]                <- same partitions, A and C never noticed
```

**Summary across scenarios** (partitions A had to give up when one member joined):

```
| scenario            | partitions A lost when B joined | partitions A lost when C joined | A's rebalance-total |
| consumer-uniform    |                               3 |                               1 |                   4 |
| consumer-range      |                               3 |                               1 |                   4 |
| classic-range       |                               6 |                               3 |                   4 |
| classic-cooperative |                               3 |                               1 |                   6 |
| consumer-static     |                               3 |                               1 |                   3 |
```

Note the pace of the consumer protocol on this stack: a revoked partition is picked up by its new owner
about 4 s later, because members learn of changes through heartbeats and the broker's
`group.consumer.heartbeat.interval.ms` is 5 s. Lower it on the broker for snappier moves at the cost of
more heartbeat traffic. The classic protocol's ~2 s gaps are the JoinGroup round (bounded by
`max.poll.interval.ms` in the worst case), during which eager members process nothing at all.

## Reading the timelines

- **Eager (classic + Range)** revokes *everything* from every member on every change and hands out a new
  set: A gives up all 6 partitions when B joins, then gets 3 back. Processing of all partitions stops
  between the two events. That gap is `rebalance-latency-avg`, and it is paid by the whole group on every
  scale-out, deploy and crash.
- **Incremental (classic + CooperativeSticky, or the consumer protocol)** revokes only what must move:
  A loses 3 partitions when B joins and keeps working on the other 3.
- **The consumer protocol** also drops the JoinGroup/SyncGroup dance: members find out about their new
  assignment in the next heartbeat, so a joiner is productive within a heartbeat interval or two, and the
  assignment logic lives on the broker (one implementation, upgraded centrally).
- **Static membership.** With `group.instance.id`, B's `close()` is not a leave; the coordinator keeps
  B's partitions reserved until `session.timeout.ms`. B restarts, presents the same instance id, and gets
  the same partitions back; A and C see nothing. The price: if B never comes back, its partitions are
  idle for the whole session timeout. Use it for stateful consumers (Kafka Streams does) and rolling deploys.
- **The deprecation warning** you see with `classic` is real: KIP-1274 phase 1 (4.3) warns, later phases
  remove it. New applications should set `group.protocol=consumer` now; the broker side is ready
  (`group.version=1` on this cluster).
- **`session.timeout.ms` vs `max.poll.interval.ms`.** The first is "is the process alive" (heartbeat thread,
  or heartbeats inside the consumer protocol); the second is "is *your* thread making progress" (chapter 07).
  A slow handler trips the second, never the first.

## When to use what

| Situation | Setting |
|---|---|
| new application on Kafka ≥ 4.0 brokers | `group.protocol=consumer`; leave `group.remote.assignor` at `uniform` |
| stuck on `classic` for a while | `partition.assignment.strategy=CooperativeStickyAssignor` (incremental, sticky) |
| co-partitioned topics that must stay aligned per consumer | `group.remote.assignor=range` (or `RangeAssignor` on classic) |
| stateful consumer, expensive local state, rolling restarts | `group.instance.id=<stable per instance>` |
| commit before losing a partition | `ConsumerRebalanceListener.onPartitionsRevoked` → `commitSync(offsets of the revoked partitions)` |
| frequent rebalances in the metrics | look for slow handlers (`max.poll.interval.ms`), crashlooping pods, and autoscalers that add/remove members too eagerly |
