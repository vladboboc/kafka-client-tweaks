package io.kafkatweaks.avro;

import io.kafkatweaks.avro.generated.Order;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * The evolved {@code Order} schemas chapter 13 registers and reads with. v1 is the schema the generated class was compiled
 * from; every other version is derived from it here, so the demos and the tests talk about exactly the same changes.
 */
public final class OrderSchemas {

    private OrderSchemas() {
    }

    /** The schema {@link Order} was generated from: what a consumer compiled against v1 reads into. */
    public static Schema v1() {
        return Order.getClassSchema();
    }

    /** Adds {@code channel}, a string WITH a default: readable in both directions (FULL). */
    public static Schema plusChannel(Schema base) {
        return withField(base, "{\"name\":\"channel\",\"type\":\"string\",\"default\":\"web\"}");
    }

    /** Adds {@code salesRep}, a string WITHOUT a default: old readers skip it, new readers cannot read old data (FORWARD only). */
    public static Schema plusSalesRep(Schema base) {
        return withField(base, "{\"name\":\"salesRep\",\"type\":\"string\"}");
    }

    /** Drops {@code customerId}, which has no default: new readers skip it, old readers miss it (BACKWARD only). */
    public static Schema minusCustomerId(Schema base) {
        return copy(base, f -> !f.name().equals("customerId"), null, null);
    }

    /** Copy of {@code base} with one field appended, given as its JSON definition. */
    public static Schema withField(Schema base, String fieldJson) {
        String json = base.toString();
        int end = json.lastIndexOf("]}");
        return new Schema.Parser().parse(json.substring(0, end) + "," + fieldJson + json.substring(end));
    }

    /** Copy of {@code base} with one field's type replaced (and its default dropped, since it would no longer fit). */
    public static Schema withFieldType(Schema base, String fieldName, Schema newType) {
        return copy(base, f -> true, fieldName, newType);
    }

    private static Schema copy(Schema base, Predicate<Schema.Field> keep, String retypedField, Schema newType) {
        var fields = new ArrayList<Schema.Field>();
        for (Schema.Field f : base.getFields()) {
            if (!keep.test(f)) {
                continue;
            }
            fields.add(f.name().equals(retypedField)
                    ? new Schema.Field(f.name(), newType, f.doc())
                    : new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
        }
        return Schema.createRecord(base.getName(), base.getDoc(), base.getNamespace(), false, fields);
    }
}
