# 18 · Error handling, retries, dead letters and `@RetryableTopic`

> **Level:** Practitioner · **Read first:** [16](16-spring-listeners-acks.md) · **Time:** ~10 min read, ~1 min run · [Glossary](glossary.md)
>
> **Demo:** `spring-error-handling` (`./demo 18`) · [ErrorHandlingDemo.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/ErrorHandlingDemo.java) · [application-spring-error-handling.yml](../spring-boot-kafka/src/main/resources/application-spring-error-handling.yml) · **Recipes:** [ErrorHandlingRecipe.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/ErrorHandlingRecipe.java), [OrderListeners.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/OrderListeners.java), [DltPublisher.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/DltPublisher.java) · **Plain-client version:** [08](08-consumer-offsets.md)
>
> **In one sentence:** Blocking retries pause the partition, `@RetryableTopic` moves the wait to retry topics at the cost of per-key ordering: an innocent record waited 3.4 s behind blocking retries, 527 ms with retry topics.

## The problem

A plain consumer that throws in its loop has three options, all hand-written: skip, retry in place, or park
the record somewhere. Chapter 08 ended with "at-least-once plus an idempotent handler"; this chapter is what
happens *before* that handler gives up. spring-kafka's container catches the exception and hands it to a
`CommonErrorHandler`. The default one retries in place with a back-off and then calls a recoverer, typically the
`DeadLetterPublishingRecoverer`. That is **blocking**: the partition waits. `@RetryableTopic` is the
**non-blocking** alternative: the failed record is re-published to a retry topic with a delay, the partition
moves on, and after the last attempt the record lands in a dead-letter topic with a `@DltHandler`. A record that
cannot even be deserialized never reaches a listener; `ErrorHandlingDeserializer` turns that into an exception
the same machinery understands.

```mermaid
flowchart LR
    P["poll()"] --> D["ErrorHandlingDeserializer<br/>(poison pill → DeserializationException)"]
    D --> L["listener"]
    L -- "throws" --> EH["DefaultErrorHandler<br/>BackOff, exception classification"]
    EH -- "retryable, attempts left" --> S["seek back, wait back-off,<br/>redeliver (partition blocked)"]
    EH -- "fatal or exhausted" --> R["DeadLetterPublishingRecoverer<br/>→ topic-dlt, same partition,<br/>kafka_dlt-* headers"]
    L2["@RetryableTopic listener"] -- "throws" --> RT["republish to<br/>topic-retry-1000 → -retry-2000 → -retry-4000"]
    RT -- "attempts exhausted" --> DLT["topic-dlt → @DltHandler"]
```

## The knobs

| Setting | Default | Meaning |
|---|---|---|
| `DefaultErrorHandler` (per container factory) | `FixedBackOff(0, 9)`: 10 attempts, no wait | the container's error handler; Boot wires a `CommonErrorHandler` **bean** into the default factory |
| `FixedBackOff(interval, maxAttempts)`, `ExponentialBackOffWithMaxRetries(n)` (+ `setInitialInterval`, `setMultiplier`, `setMaxInterval`) | | how long the partition waits between attempts |
| `addNotRetryableExceptions(...)` / `addRetryableExceptions(...)` | fatal: `DeserializationException`, `MessageConversionException`, `ConversionException`, `MethodArgumentResolutionException`, `NoSuchMethodException`, `ClassCastException` | classification; the handler looks through `ListenerExecutionFailedException` at the cause |
| `setResetStateOnExceptionChange` | `true` | a different exception restarts the back-off sequence |
| `setSeekAfterError` | `true` | re-seek to redeliver (false keeps the records in memory) |
| `DeadLetterPublishingRecoverer(template)` | destination `<topic>-dlt`, same partition | publishes the failed record with headers `kafka_dlt-original-topic/partition/offset/timestamp`, `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace`; a `BiFunction` resolver changes the destination |
| the recoverer's template | | must serialize what it gets: the original `byte[]` after a deserialization failure, the deserialized object otherwise → `DelegatingByTypeSerializer` |
| `ContainerProperties.setDeliveryAttemptHeader(true)` | `false` | adds `kafka_deliveryAttempt` (int) to every delivery |
| `spring.kafka.consumer.value-deserializer: ErrorHandlingDeserializer` + `properties["[spring.deserializer.value.delegate.class]"]` | | a deserializer that never throws: the record arrives with a `null` value and an exception header; the handler treats it as fatal |
| `BatchListenerFailedException(msg, record)` | | batch listeners: tells the handler which record failed, so only it and the rest of the batch are redelivered |
| `CommonContainerStoppingErrorHandler`, `CommonLoggingErrorHandler`, `CommonDelegatingErrorHandler` | | stop the container / log and skip / pick a handler per exception type |
| `@RetryableTopic(attempts, backOff = @BackOff(delay, multiplier, maxDelay), include/exclude, numPartitions, replicationFactor, autoCreateTopics, topicSuffixingStrategy, sameIntervalTopicReuseStrategy, dltStrategy, kafkaTemplate)` | 3 attempts, 1 s, 1 partition, broker default RF | non-blocking retries; topics `<topic>-retry-<delay>` and `<topic>-dlt`; needs a `KafkaTemplate` bean (the single one, or named) |
| `@DltHandler` | log only | the method that receives what exhausted its attempts; the exception travels in `kafka_exception-*` headers |
| `spring.kafka.retry.topic.enabled` + `attempts`, `backoff.*` | off | Boot's global alternative: the same for *every* listener, no annotation |
| limits | | `@RetryableTopic` does not combine with batch listeners or with container transactions (chapter 19) |

## The code that matters

Blocking retries are a container factory with an error handler, from
[ErrorHandlingRecipe.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/ErrorHandlingRecipe.java):

<!-- recipe: spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/ErrorHandlingRecipe.java -->
```java
var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
configurer.configure(factory, consumerFactory);
var backOff = new ExponentialBackOffWithMaxRetries(3);
backOff.setInitialInterval(200);
backOff.setMultiplier(2.0);
backOff.setMaxInterval(2000);
// DeadLetterPublishingRecoverer defaults: destination "<topic>-dlt", same partition as the original record.
var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(dlt.template()), backOff);
handler.addNotRetryableExceptions(IllegalArgumentException.class);
factory.setCommonErrorHandler(handler);
```

non-blocking retries are an annotation, from [OrderListeners.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/OrderListeners.java)
(`orders` is your service; in the demo it fails on cue and records every attempt):

<!-- recipe: spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/OrderListeners.java -->
```java
@KafkaListener(id = "errors-blocking", groupId = "spring-errors-blocking", clientIdPrefix = "errors-blocking",
        topics = TopicsConfig.ERRORS, containerFactory = "blockingRetryFactory")
public void blocking(ConsumerRecord<String, Order> record) {
    orders.handle(record);
}

// attempts = 1 delivery + 3 retries; numPartitions: the retry topics default to ONE partition, set it
@RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2.0), include = TransientFailure.class,
        numPartitions = "3", autoCreateTopics = "true")
@KafkaListener(id = "errors-retryable", groupId = "spring-errors-retryable", clientIdPrefix = "errors-retryable", topics = TopicsConfig.RETRYABLE)
public void retryable(ConsumerRecord<String, Order> record) {
    orders.handle(record);
}

/** What exhausted its attempts arrives here; the exception travels in the {@code kafka_exception-*} headers. */
@DltHandler
public void parked(ConsumerRecord<String, Order> record) {
    orders.parked(record);
}
```

and the poison-pill half is two lines of yml (`value-deserializer: ErrorHandlingDeserializer` plus its delegate),
[application-spring-error-handling.yml](../spring-boot-kafka/src/main/resources/application-spring-error-handling.yml).

- **Blocking**: `flaky9-6` was tried at 1016, 1520, 2344 and 3155 ms and dead-lettered with its exception in the
  `kafka_dlt-*` headers; `fatal-4` (`IllegalArgumentException`, not retryable) went to the DLT after one attempt.
  The consumer, and with it every partition it owned, waited meanwhile: an innocent record took 3.4 s.
- **Non-blocking**: the same failure moved through `-retry-1000`, `-retry-2000`, `-retry-4000` and `-dlt` while
  the partition moved on; the innocent record took 527 ms. Ordering per key is the price.
- **The DLT template** ([DltPublisher.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/recipe/DltPublisher.java))
  needs a `DelegatingByTypeSerializer`: a poison pill reaches the recoverer as the original `byte[]`, a failed
  record as the deserialized object.

<details>
<summary>Deep dive: why the error handler is not a bean</summary>

- **The handler is not a bean on purpose**: Boot would wire a `CommonErrorHandler` bean into the default factory
  and the `@RetryableTopic` listener must keep its own.

</details>

The demo produces scripted keys and reads the DLT back; everything else in
[ErrorHandlingDemo.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/ErrorHandlingDemo.java) and
[ScriptedOrderHandler.java](../spring-boot-kafka/src/main/java/io/kafkatweaks/spring/errors/ScriptedOrderHandler.java)
is measurement.

## Run it

```bash
./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="spring-error-handling"
```

The record keys script the failures (`FailureScript`): `ok-n` succeeds, `flaky<k>-n` throws a retryable
`TransientFailure` on the first k attempts, `fatal-n` throws `IllegalArgumentException` (registered as not
retryable), `poison-n` has a value that is not JSON.

## What you should see

**1. Blocking retries.** Eight records, `ExponentialBackOffWithMaxRetries(3)` at 200/400/800 ms, JSON values,
`ErrorHandlingDeserializer` in front of `JacksonJsonDeserializer`:

```
| t ms | partition | key                                 | attempt | kafka_deliveryAttempt header | ms since send | listener                        |
|  474 |         2 | ok-1  (succeeds)                    |       1 |                            1 |           379 | processed                       |
| 1016 |         1 | ok-5  (succeeds)                    |       1 |                            1 |           794 | processed                       |
| 1016 |         1 | flaky9-6  (fails 9x, then succeeds) |       1 |                            1 |           775 | throws TransientFailure         |
| 1225 |         0 | ok-2  (succeeds)                    |       1 |                            1 |          1036 | processed                       |
| 1225 |         0 | flaky2-3  (fails 2x, then succeeds) |       1 |                            1 |          1021 | throws TransientFailure         |
| 1520 |         1 | flaky9-6  (fails 9x, then succeeds) |       2 |                            2 |          1279 | throws TransientFailure         |
| 1930 |         0 | flaky2-3  (fails 2x, then succeeds) |       2 |                            2 |          1726 | throws TransientFailure         |
| 2344 |         1 | flaky9-6  (fails 9x, then succeeds) |       3 |                            3 |          2103 | throws TransientFailure         |
| 3155 |         1 | flaky9-6  (fails 9x, then succeeds) |       4 |                            4 |          2914 | throws TransientFailure         |
| 3183 |         0 | flaky2-3  (fails 2x, then succeeds) |       3 |                            3 |          2979 | processed                       |
| 3183 |         0 | fatal-4  (not retryable)            |       1 |                            1 |          2970 | throws IllegalArgumentException |
| 3687 |         0 | ok-7  (succeeds)                    |       1 |                            1 |          3437 | processed                       |
```

and the dead-letter topic afterwards:

```
| DLT key  | value                                 | kafka_dlt-exception-fqcn         | kafka_dlt-exception-cause-fqcn | original topic-partition@offset |
| fatal-4  | {"orderId":"ORD-2475","customerId"... | ListenerExecutionFailedException | IllegalArgumentException       | spring.errors-0@2               |
| flaky9-6 | {"orderId":"ORD-41977","customerId... | ListenerExecutionFailedException | TransientFailure               | spring.errors-1@1               |
| poison-8 | {not json                             | DeserializationException         | DeserializationException       | spring.errors-2@1               |
```

**2. Non-blocking retries** with `@RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2.0))`:

```
| t ms  | topic       | partition | key                                 | attempt | retry_topic-attempts header | ms since original send | listener                                      |
|  4877 | main        |         2 | ok-1  (succeeds)                    |       1 |                           - |                     11 | processed                                     |
|  4887 | main        |         0 | flaky2-2  (fails 2x, then succeeds) |       1 |                           - |                     10 | throws TransientFailure                       |
|  4917 | main        |         2 | flaky9-3  (fails 9x, then succeeds) |       1 |                           - |                     31 | throws TransientFailure                       |
|  5422 | main        |         0 | ok-4  (succeeds)                    |       1 |                           - |                    527 | processed                                     |
|  5896 | -retry-1000 |         0 | flaky2-2  (fails 2x, then succeeds) |       2 |                           2 |                   1019 | throws TransientFailure                       |
|  5921 | -retry-1000 |         2 | flaky9-3  (fails 9x, then succeeds) |       2 |                           2 |                   1035 | throws TransientFailure                       |
|  7902 | -retry-2000 |         0 | flaky2-2  (fails 2x, then succeeds) |       3 |                           3 |                   3025 | processed                                     |
|  7925 | -retry-2000 |         2 | flaky9-3  (fails 9x, then succeeds) |       3 |                           3 |                   3039 | throws TransientFailure                       |
| 11.9K | -retry-4000 |         2 | flaky9-3  (fails 9x, then succeeds) |       4 |                           4 |                   7045 | throws TransientFailure                       |
| 12.4K |        -dlt |         2 | flaky9-3  (fails 9x, then succeeds) |       4 |                           5 |                   7561 | @DltHandler: ListenerExecutionFailedException |
```

**3. What the innocent records paid:**

```
| strategy              | slowest 'ok' record (ms from send to processing) | why                                                                      |
| blocking (part 1)     |                                             3437 | one consumer holds all 3 partitions, so every back-off stalls them all   |
| non-blocking (part 2) |                                              527 | the failed record leaves the partition; ok records are processed at once |
```

## Reading the numbers

- **Blocking retry pauses the consumer, not just the record.** `flaky9-6` was tried at 1016, 1520, 2344 and
  3155 ms: the 200, 400 and 800 ms back-offs. The container runs one consumer (the default concurrency 1) for all
  three partitions and waits out every back-off on its thread, so each back-off stalled every partition it owned:
  `flaky2-3`'s retries in partition 0 only ran in between `flaky9-6`'s, and `ok-7`, queued behind `flaky2-3` and
  `fatal-4` in partition 0, was processed after 3.4 s. With one consumer per partition (`concurrency` 3, chapter 17)
  only the failing record's partition would wait. Back-off is cheap for the failing record and expensive for its neighbours, which is why the default
  handler retries **ten times with no wait**: fast failures for transient blips, nothing else.
- **Classification decides between retry and dead letter.** `IllegalArgumentException` was registered as not
  retryable and went to the DLT after one attempt. The handler classifies by the *cause* inside
  `ListenerExecutionFailedException`, so throw meaningful exceptions from listeners. The
  `kafka_dlt-exception-cause-fqcn` header shows the real reason; `-fqcn` shows Spring's wrapper.
- **A poison pill never reaches the listener.** `ErrorHandlingDeserializer` returned a `null` value with a
  `DeserializationException` in a header; the container turned that into a fatal error, and the recoverer
  published the **original bytes** to the DLT. Without it the consumer would throw on every poll and the
  partition would be stuck forever. The recoverer's template needs a serializer that accepts `byte[]` as well as
  the normal value type (`DelegatingByTypeSerializer`).
- **Same partition, same key, a lot of context.** The DLT record keeps the key, so a DLT consumer can still
  group by entity, and the headers say where it came from (`spring.errors-0@2`) and why. A DLT is an
  operational queue, not a bit bucket: something has to read it (chapter 20's share consumers are a good fit).
- **Non-blocking retry moves the wait off the partition.** `flaky2-2` failed at 4887 ms, was re-published to
  `-retry-1000` and processed there at 5896 ms; `ok-4` behind it was processed after 527 ms instead of seconds.
  Each retry topic has its own container (five listener ids in the output), the attempt count travels in
  `retry_topic-attempts`, and the final record reaches `@DltHandler` with `kafka_exception-*` headers. The price:
  ordering per key is gone (a retried record is processed after its successors), the record is copied once per
  attempt, and topics multiply (`numPartitions` defaults to 1: set it).
- **Both are at-least-once.** A retried record was seen by the listener before; the DLT record may be a
  duplicate of a side effect that half happened. Chapter 08's idempotent handler still applies.

## Key takeaways

- **Blocking retry pauses the consumer's partitions.** `DefaultErrorHandler` seeks back and waits out each back-off on
  the consumer thread; an innocent record waited 3.4 s. Keep blocking back-offs short.
- **`@RetryableTopic` moves the wait off the partition, and ordering with it.** The innocent record took 527 ms; a
  retried record is processed after its successors and copied per attempt.
- **Classify, and catch poison pills before the listener.** Register bugs as not retryable so they go straight to the
  DLT; `ErrorHandlingDeserializer` plus a `DelegatingByTypeSerializer` DLT template parks undeserializable bytes.

## When to use what

| Situation | Setting |
|---|---|
| transient failures (timeouts, 5xx) that clear in milliseconds | default `DefaultErrorHandler` or `FixedBackOff(100, 3)`; keep the partition pause short |
| transient failures that need seconds to minutes | `@RetryableTopic` with exponential back-off; accept the loss of ordering |
| bugs and bad data | `addNotRetryableExceptions(...)` so they go straight to the DLT; alert on the DLT |
| records that cannot be deserialized | `ErrorHandlingDeserializer` + a DLT recoverer with `DelegatingByTypeSerializer` |
| order per key must survive failures | blocking retries only; no `@RetryableTopic` |
| a downstream outage: stop instead of burning through records | `CommonContainerStoppingErrorHandler` (and chapter 17's pause) |
| batch listeners | throw `BatchListenerFailedException(msg, record)` from the loop, so the handler knows the index |
| the same policy for every listener, no annotations | `spring.kafka.retry.topic.enabled=true` + `spring.kafka.retry.topic.*` |
| exactly-once processor (chapter 19) | blocking retries via `DefaultAfterRollbackProcessor`; `@RetryableTopic` is not compatible with container transactions |

---

← [17 · Concurrency, batch listeners and back-pressure](17-spring-concurrency-batch.md) · [Index](README.md) · [19 · Transactions in Spring](19-spring-transactions.md) →
