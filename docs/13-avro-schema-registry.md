# 13 · Serialization with the Schema Registry and Avro

> **Level:** Practitioner · **Read first:** [01](01-producer-baseline.md), [07](07-consumer-fetch.md) · **Time:** ~5 min read, ~1 min run · [Glossary](glossary.md)
>
> **Demo:** `avro-roundtrip` (`./demo 13`) · [AvroDemo.java](../plain-clients/src/main/java/io/kafkatweaks/avro/AvroDemo.java) · schema: [Order.avsc](../tweaks-common/src/main/avro/Order.avsc) · **Recipes:** [AvroClients.java](../plain-clients/src/main/java/io/kafkatweaks/avro/recipe/AvroClients.java), [AvroTrust.java](../tweaks-common/src/main/java/io/kafkatweaks/avro/AvroTrust.java) · **In Spring:** [21](21-spring-serialization.md)
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
v3 in; the demo restores `BACKWARD` afterwards.

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

## Key takeaways

- **Avro ships values, not field names**: 37.39 bytes per record against 129 for JSON, behind a 5-byte header
  (magic byte + schema id).
- **Production producers run `auto.register.schemas=false` + `use.latest.version=true`**: schemas are registered
  from CI, and a send against an unknown subject fails instead of creating a version.
- **Compatibility is checked per subject, at registration**: under `BACKWARD` a new field needs a default; v3
  without one was rejected with HTTP 409.

## When to use what

| Situation | Setting |
|---|---|
| production producers | `auto.register.schemas=false`, `use.latest.version=true`; register from CI with compatibility checks |
| several event types on one topic (per-entity streams) | `RecordNameStrategy` or `TopicRecordNameStrategy` |
| consumers deploy before producers | `BACKWARD` (default) |
| producers deploy before consumers | `FORWARD` |
| independent deploys both ways | `FULL` (+ `_TRANSITIVE` when consumers may lag several versions) |
| generated classes on the consumer | `specific.avro.reader=true` + `AvroTrust` |
| dynamic consumers (routers, archivers) | `GenericRecord` (`specific.avro.reader=false`), no class trust needed |

---

← [12 · Client resilience and operations](12-client-resilience.md) · [Index](README.md) · [14 · Spring Boot wiring and the `spring.kafka.*` mapping](14-spring-boot-setup.md) →
