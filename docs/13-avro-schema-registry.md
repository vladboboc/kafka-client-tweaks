# 13 · Serialization with the Schema Registry and Avro

> **Level:** Practitioner · **Read first:** [01](01-producer-baseline.md), [07](07-consumer-fetch.md) · **Time:** ~10 min read, ~1 min per run · [Glossary](glossary.md)
>
> **Demos:** `avro-roundtrip` (`./demo 13`) · [AvroDemo.java](../plain-clients/src/main/java/io/kafkatweaks/avro/AvroDemo.java); `avro-compatibility` (`./demo avro-compatibility`) · [AvroCompatibilityDemo.java](../plain-clients/src/main/java/io/kafkatweaks/avro/AvroCompatibilityDemo.java) · schema: [Order.avsc](../tweaks-common/src/main/avro/Order.avsc) · **Recipes:** [AvroClients.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java), [SchemaPipeline.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/SchemaPipeline.java), [AvroTrust.java](../tweaks-common/src/main/java/io/kafkatweaks/avro/AvroTrust.java) · **In Spring:** [21](21-spring-serialization.md)
>
> **In one sentence:** Avro with the Schema Registry puts 37.39 bytes per record on the wire against 129 for JSON, and the registry rejects incompatible schema changes at registration, before any consumer sees them.

## The problem

Every chapter so far shipped strings. Real topics carry structured events that several teams produce and
consume for years, and two things go wrong with JSON there: every record repeats its field names (bytes,
CPU), and nothing stops a producer from renaming a field and breaking every consumer at 3 a.m. The Schema
Registry fixes the second by making the schema a versioned, checked contract; Avro fixes the first by
shipping only values.

## The wire format

```
+------+----------+----------------------------------+
| 0x00 | schema id| Avro binary (values only)        |
| 1 B  | 4 B      | ...                              |
+------+----------+----------------------------------+
```

The serializer registers (or looks up) the schema under a **subject** (`<topic>-value` by default), gets
an id, caches it, and prefixes every record with it. The deserializer reads the id, fetches the writer
schema once, caches it, and decodes. Neither side sends the schema itself.

## The knobs

| Config | Default | Meaning |
|---|---|---|
| `schema.registry.url` | – | comma-separated registry URLs |
| `auto.register.schemas` | `true` | serializer registers unknown schemas. **`false` in production**: register from CI/CD, so a code change cannot silently create a new version |
| `use.latest.version` | `false` | serialize with the subject's latest registered schema rather than the object's own |
| `latest.compatibility.strict` | `true` | with `use.latest.version`, refuse if the object's schema is not compatible with that latest version |
| `value.subject.name.strategy` | `TopicNameStrategy` | `<topic>-value`. `RecordNameStrategy`: `<record full name>` (many event types per topic). `TopicRecordNameStrategy`: both |
| `specific.avro.reader` (deserializer) | `false` | `true` = instantiate the generated class; `false` = `GenericRecord` |
| `normalize.schemas` | `false` | ignore irrelevant ordering when comparing schemas |
| subject compatibility (registry) | `BACKWARD` | `BACKWARD`, `FORWARD`, `FULL`, their `_TRANSITIVE` variants, `NONE` |

<details>
<summary>Deep dive: how the generated Order class is built</summary>

Build side: `avro-maven-plugin` generates `io.kafkatweaks.avro.generated.Order` from the `.avsc` with
`stringType=String` and `enableDecimalLogicalType=true` (`BigDecimal` instead of `ByteBuffer`).

</details>

## The code that matters

The production producer, from [AvroClients.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java):

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java -->
```java
public static Map<String, Object> productionProducer(String schemaRegistryUrl) {
    var config = new HashMap<>(producer(schemaRegistryUrl));
    // Default true: any refactor of the class silently becomes a new schema version, registered by whichever
    // service happens to deploy first.
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
    // Serialize with the subject's latest registered version instead of the class's own schema. With the default
    // latest.compatibility.strict=true the serializer refuses if the class is not compatible with that version.
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
    return config;
}
```

and the one call Avro ≥ 1.12.1 needs before the first generated class is serialized or read, from
[AvroTrust.java](../tweaks-common/src/main/java/io/kafkatweaks/avro/AvroTrust.java):

<!-- recipe: tweaks-common/src/main/java/io/kafkatweaks/avro/AvroTrust.java -->
```java
var previous = ClassSecurityValidator.getGlobal();
var generated = ClassSecurityValidator.builder().add(Order.class).build();
ClassSecurityValidator.setGlobal(clazz -> previous.isTrusted(clazz) || generated.isTrusted(clazz));
```

- **`producer(url)`** alone (`KafkaAvroSerializer` + `schema.registry.url`) gave 37.39 bytes per record against 129
  for JSON: a 5-byte header (magic byte + schema id) and Avro binary without field names.
- **`productionProducer(url)`** is the pair to run with: schemas are registered by a pipeline, and a send against an
  unknown subject fails (`Subject ... not found`) instead of registering whatever the code happens to contain.
- **Trust the generated classes once, at startup** (`AvroTrust.trustGeneratedClasses()`): the first send below fails
  with `SecurityException: Forbidden ...` until they are. It is a JVM-wide switch, so it is a call of its own and not
  part of any config map.
- Consumers: `specificConsumer(url)` for the generated classes, `latestSchemaConsumer(url)` for `GenericRecord`s in
  the subject's latest shape (old records get the new fields' defaults).

The demo's producers and consumers are built from these maps; everything else in
[AvroDemo.java](../plain-clients/src/main/java/io/kafkatweaks/avro/AvroDemo.java) is measurement and schema evolution
against the registry.

## Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="avro-roundtrip"
```

Arguments: `records=1000`. Subjects are listed at http://localhost:8081/subjects and in the UI.

## What you should see

**1. Round trip.** The first attempt to produce a generated `Order` fails before the class is trusted:

```
FAILED before AvroTrust: SecurityException: Forbidden io.kafkatweaks.avro.generated.Order! This class is not trusted to be included in Avro schemas. ...
after AvroTrust.trustGeneratedClasses():
registered: subject tweaks.avro-value, schema id 2, version 1
first send: 239 ms (schema registration + lookup over HTTP), later sends: 11.03 ms avg (schema id cached in the serializer)
```

(Schema ids are global and grow with every distinct schema ever registered; versions count per subject.
Your ids will differ.)

```

| format         | bytes/record | what is on the wire                                               |
| Confluent Avro |        37.39 | magic byte 0x00 + 4-byte schema id + Avro binary (no field names) |
| JSON (Jackson) |          129 | field names + values as text, no schema id                        |
```

then 1 000 records come back through `KafkaAvroDeserializer` with `specific.avro.reader=true` as `Order` instances.

**2. Serializer knobs:**

```
| setting                                                | outcome                                                                                   |
| auto.register.schemas=false, subject has no schema yet | send fails: Subject 'tweaks.avro-noreg-value' not found.; error code: 40401               |
| value.subject.name.strategy=RecordNameStrategy         | registered under subject 'io.kafkatweaks.avro.generated.Order'                            |
| auto.register.schemas=false + use.latest.version=true  | send OK: uses the registry's latest version of tweaks.avro-value (the production setting) |
subjects now in the registry: [io.kafkatweaks.avro.generated.Order, tweaks.avro-value]
```

**3. Evolution** against the subject's `BACKWARD` compatibility:

```
| candidate schema                       | compatible with v1 under BACKWARD? | register                                          |
| v2 = v1 + channel:string default "web" | true                               | OK, id <next>, version 2                          |
| v3 = v1 + salesRep:string (NO default) | false                              | REJECTED 409/40901: READER_FIELD_MISSING_DEFAULT_VALUE |
| v4 = v1 with currency: string -> int   | false                              | REJECTED 409/40901: TYPE_MISMATCH                 |
```

Reading the v1 records with v2 as the reader schema (`use.latest.version=true` on a `GenericRecord`
consumer) shows the new `channel` field filled with its default `web`. Setting the subject to `NONE` lets
v3 in; the demo restores `BACKWARD` afterwards. [Writer schema vs reader schema](#writer-schema-vs-reader-schema-backward-vs-forward)
below goes through this record by record, in both directions.

## Reading the numbers

- **Avro is a fraction of JSON on the wire** for the same order: no field names, binary numbers, 5 bytes
  of header. Multiply by every record, every replica, every consumer.
- **The first send is slow, the rest are not.** Registration/lookup is an HTTP round trip once per
  schema per serializer instance; after that the id is cached. Deserializers cache by id the same way.
- **`SecurityException: Forbidden io.kafkatweaks.avro.generated.Order`** is Avro ≥ 1.12.1 refusing to
  resolve a class by name (CVE-2024-47561 hardening). With the Confluent 8.3 serializers it fires on both
  sides: the serializer resolves the record's class to its schema (the demo's first send fails before
  `AvroTrust`), and `specific.avro.reader=true` resolves the schema's name to a class. `AvroTrust` adds the
  generated classes to Avro's `ClassSecurityValidator`, the live validator that replaced the read-once
  `org.apache.avro.SERIALIZABLE_PACKAGES` system property in Avro 1.12.2. Only `GenericRecord` consumers,
  which never instantiate generated classes, are unaffected.
- **`auto.register.schemas=false` + `use.latest.version=true`** is the production pair: schemas are
  registered by a pipeline, the application can only produce what is already agreed. The demo shows the
  failure you get when the subject is empty.
- **Compatibility is per subject and checked at registration**, not at produce time. Under `BACKWARD` a
  field can be added only with a default (so that the new reader can read old data), and removed freely.
  A required field without a default is rejected with HTTP 409. `NONE` accepts anything and shifts the
  failure to consumers at read time.
- **Old data, new shape**: reading v1 records with v2 as reader schema fills the new `channel` field from
  its default. That is what backward compatibility buys: deploy consumers first, producers later.

## Writer schema vs reader schema: BACKWARD vs FORWARD

> **Demo:** `avro-compatibility` (`./demo avro-compatibility`) · [AvroCompatibilityDemo.java](../plain-clients/src/main/java/io/kafkatweaks/avro/AvroCompatibilityDemo.java) · **Recipe:** [SchemaPipeline.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/SchemaPipeline.java), the two consumers in [AvroClients.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java)

Every Avro record is decoded with two schemas:

- **Writer schema**: the one the producer serialized with. Its id travels in bytes 1-4 of the record (see
  [the wire format](#the-wire-format)); the deserializer fetches the schema from the registry by that id.
- **Reader schema**: the shape the consumer wants. With `specific.avro.reader=true` it is the schema the generated
  class was compiled from (`Order` is v1), looked up by the writer schema's full name, so a record's name and
  namespace must never change. With `use.latest.version=true` it is the subject's latest version. With neither, it
  is the writer schema itself: a `GenericRecord` in whatever shape was written.

Avro resolves the writer schema into the reader schema field by field, by name. A writer field the reader does not
know is skipped. A reader field the writer did not have is filled from its default, and without a default the
record cannot be read. That one rule is all compatibility is about. The level names the direction the registry
checks, seen from the NEW schema:

| Level | The registry checks that... | So it is safe to |
|---|---|---|
| `BACKWARD` (default) | the new schema, as reader, can read data written with the previous one | upgrade consumers first |
| `FORWARD` | the previous schema, as reader, can read data written with the new one | upgrade producers first |
| `FULL` | both | deploy in any order |

So "a consumer compiled with v1 reads records whose schema id is v2" is the **FORWARD** direction (old reader, new
writer). It works for the usual change, adding a field with a default, because that change is `FULL`: it passes
both checks. `BACKWARD` alone does not promise it. The demo registers a v3 that `BACKWARD` accepts and that the v1
consumer cannot read.

### The code that matters

The two reader-schema choices, from [AvroClients.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java):

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java -->
```java
public static Map<String, Object> specificConsumer(String schemaRegistryUrl) {
    return Map.of(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
}
// ...
public static Map<String, Object> latestSchemaConsumer(String schemaRegistryUrl) {
    return Map.of(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
}
```

and the pipeline side, from [SchemaPipeline.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/SchemaPipeline.java):

<!-- recipe: plain-clients/src/main/java/io/kafkatweaks/avro/recipe/SchemaPipeline.java -->
```java
public static void pinCompatibility(SchemaRegistryClient registry, String subject, String level)
        throws IOException, RestClientException {
    // BACKWARD (registry default): the new schema can read data written with the previous one. Upgrade consumers first.
    // FORWARD: the previous schema can read data written with the new one. Upgrade producers first.
    // FULL: both. Add _TRANSITIVE to check against every registered version instead of only the latest.
    registry.updateCompatibility(subject, level);
}
// ...
public static SchemaCompatibility.SchemaPairCompatibility canRead(Schema reader, Schema writer) {
    // BACKWARD asks canRead(newSchema, oldSchema); FORWARD asks canRead(oldSchema, newSchema).
    return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
}
```

- **`specificConsumer`** is the consumer compiled with v1. It keeps working as long as every version written to the
  topic is `FORWARD` (or `FULL`) compatible with v1.
- **`latestSchemaConsumer`** reads every record in the newest shape. It needs `BACKWARD`: old records must fit the
  new schema.
- **`pinCompatibility`** is the knob, per subject. `SchemaPipeline.register` is the CI step that the level guards
  (producers run `auto.register.schemas=false`). **`canRead`** runs Avro's own check with no registry, so the
  producer's build can test a schema change before it gets anywhere near the registry
  ([SchemaPipelineTest](../plain-clients/src/test/java/io/kafkatweaks/avro/recipe/SchemaPipelineTest.java) asserts
  the matrix below).

### Run it

```bash
./mvnw -q -pl plain-clients -am compile exec:java -Dexec.args="avro-compatibility"
```

No arguments. It recreates `tweaks.avro-compat-backward` and `tweaks.avro-compat-forward` (one partition each) and
deletes their subjects first, so every run starts at version 1.

### What you should see

**1. Three changes, both directions**, Avro only (`SchemaPipeline.canRead`):

```
| change to v1                 | new reader, old data (BACKWARD)                   | old reader, new data (FORWARD)                      | satisfies     |
| add channel (default "web")  | yes                                               | yes                                                 | FULL          |
| add salesRep (no default)    | no: READER_FIELD_MISSING_DEFAULT_VALUE (salesRep) | yes                                                 | FORWARD only  |
| drop customerId (no default) | yes                                               | no: READER_FIELD_MISSING_DEFAULT_VALUE (customerId) | BACKWARD only |
```

**2. Subject at `BACKWARD`.** The pipeline registers v1, v2 and v3. The "add salesRep" candidate is rejected:

```
| candidate                             | register under BACKWARD                                |
| v1 (the compiled Order)               | OK, id 1, version 1                                    |
| v2 = v1, add channel (default "web")  | OK, id 5, version 2                                    |
| v2, add salesRep (no default)         | REJECTED 409/40901: READER_FIELD_MISSING_DEFAULT_VALUE |
| v3 = v2, drop customerId (no default) | OK, id 6, version 3                                    |
```

One record is produced with each writer schema, then two consumers read the topic:

```
reader schema v1: compiled Order class
| offset | writer schema (id in the record) | result                                          |
|      0 | id 1 = v1                        | OK  Order(orderId=ORD-1, customerId=customer-1) |
|      1 | id 5 = v2                        | OK  Order(orderId=ORD-2, customerId=customer-2) |
|      2 | id 6 = v3                        | FAILED  missing required field customerId       |

reader schema latest registered: v3
| offset | writer schema (id in the record) | result                                           |
|      0 | id 1 = v1                        | OK  GenericRecord(orderId=ORD-1, channel=web)    |
|      1 | id 5 = v2                        | OK  GenericRecord(orderId=ORD-2, channel=mobile) |
|      2 | id 6 = v3                        | OK  GenericRecord(orderId=ORD-3, channel=mobile) |
```

**3. Subject at `FORWARD`.** The same steps. This time the drop is rejected and the field without a default
gets in:

```
| candidate                            | register under FORWARD                                 |
| v1 (the compiled Order)              | OK, id 1, version 1                                    |
| v2 = v1, add channel (default "web") | OK, id 5, version 2                                    |
| v2, drop customerId (no default)     | REJECTED 409/40901: READER_FIELD_MISSING_DEFAULT_VALUE |
| v3 = v2, add salesRep (no default)   | OK, id 7, version 3                                    |

reader schema v1: compiled Order class
| offset | writer schema (id in the record) | result                                          |
|      0 | id 1 = v1                        | OK  Order(orderId=ORD-1, customerId=customer-1) |
|      1 | id 5 = v2                        | OK  Order(orderId=ORD-2, customerId=customer-2) |
|      2 | id 7 = v3                        | OK  Order(orderId=ORD-3, customerId=customer-3) |

reader schema latest registered: v3
| offset | writer schema (id in the record) | result                                                                                 |
|      0 | id 1 = v1                        | FAILED  missing required field salesRep                                                |
|      1 | id 5 = v2                        | FAILED  missing required field salesRep                                                |
|      2 | id 7 = v3                        | OK  GenericRecord(orderId=ORD-3, customerId=customer-3, channel=mobile, salesRep=SR-7) |
```

Schema ids are global: the same schema text gets the same id in both subjects (v1 is id 1 in both), and a schema
id is never reused after its subject is deleted. Your ids will differ.

### Reading the numbers

- **The v1 consumer decides nothing up front.** It takes the writer schema from each record's id and resolves it
  into the compiled class. Record 1 (writer v2) was read under both levels: the extra `channel` was skipped.
- **`BACKWARD` approved v3, and the v1 consumer broke on it** (`missing required field customerId`). `BACKWARD`
  only checks new-reads-old. That is safe if every consumer is upgraded before any producer writes v3. The
  latest-schema consumer read all three records and filled `channel=web` into the v1 record.
- **`FORWARD` rejected the same drop and let `salesRep` in without a default.** The v1 consumer read all three
  records. The consumer that adopted v3 cannot read v1 or v2 records: think of a replay, a new group starting from
  `earliest`, or a partition that lags behind. Under `FORWARD`, producers go first, and consumers move to the new
  reader schema only once the old data has aged out of retention.
- **In a real `KafkaConsumer` a FAILED row is a stuck partition.** `poll()` throws `RecordDeserializationException`
  at that offset and keeps throwing until the application seeks past it (in Spring: `ErrorHandlingDeserializer`,
  [chapter 18](18-spring-error-handling-retry.md)). The demo calls the configured `KafkaAvroDeserializer` itself,
  record by record, which is what the consumer does internally. That way it can show each record's schema id and
  carry on after a failure.
- **`add channel` passed every check.** Adding or removing a field *with a default* is the change both old and new
  readers survive. `FULL` makes the registry enforce exactly that. Each check compares against the latest version
  only (v3 was checked against v2). Use the `_TRANSITIVE` variants when consumers may be several versions behind.

## Key takeaways

- **Avro ships values, not field names**: 37.39 bytes per record against 129 for JSON, behind a 5-byte header
  (magic byte + schema id).
- **Production producers run `auto.register.schemas=false` + `use.latest.version=true`**: schemas are registered
  from CI, and a send against an unknown subject fails instead of creating a version.
- **The level names the direction the registry checks**: `BACKWARD` = the new schema reads old data (upgrade
  consumers first), `FORWARD` = the old schema reads new data (producers first). A consumer compiled with v1 reads
  v2 records only if v2 is `FORWARD` or `FULL` compatible. `BACKWARD` accepted a v3 that this consumer could not read.

## When to use what

| Situation | Setting |
|---|---|
| production producers | `auto.register.schemas=false`, `use.latest.version=true`; register from CI with compatibility checks |
| several event types on one topic (per-entity streams) | `RecordNameStrategy` or `TopicRecordNameStrategy` |
| consumers deploy before producers | `BACKWARD` (default) |
| producers deploy before consumers | `FORWARD` |
| consumers compiled against an old class must keep reading new records | `FORWARD` or `FULL` |
| consumers read everything, old records included, with the newest schema | `BACKWARD` or `FULL` |
| independent deploys both ways | `FULL` (+ `_TRANSITIVE` when consumers may lag several versions) |
| generated classes on the consumer | `specific.avro.reader=true` + `AvroTrust` |
| dynamic consumers (routers, archivers) | `GenericRecord` (`specific.avro.reader=false`), no class trust needed |

---

← [12 · Client resilience and operations](12-client-resilience.md) · [Index](README.md) · [14 · Spring Boot wiring and the `spring.kafka.*` mapping](14-spring-boot-setup.md) →
