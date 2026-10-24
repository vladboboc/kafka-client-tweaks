package io.kafkatweaks.avro.recipe;

import io.kafkatweaks.avro.OrderSchemas;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The reader x writer matrix of chapter 13, with Avro's resolution rules only: no registry, no broker. */
class SchemaPipelineTest {

    private static final Schema V1 = OrderSchemas.v1();

    @Test
    void addingAFieldWithADefaultIsReadableBothWays() {
        Schema v2 = OrderSchemas.plusChannel(V1);
        assertThat(type(v2, V1)).as("new reader, old data").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
        assertThat(type(V1, v2)).as("old reader, new data").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void addingAFieldWithoutADefaultIsForwardOnly() {
        Schema v2 = OrderSchemas.plusSalesRep(V1);
        assertThat(type(v2, V1)).as("new reader, old data").isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
        assertThat(type(V1, v2)).as("old reader, new data").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void droppingAFieldWithoutADefaultIsBackwardOnly() {
        Schema v2 = OrderSchemas.minusCustomerId(V1);
        assertThat(type(v2, V1)).as("new reader, old data").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
        assertThat(type(V1, v2)).as("old reader, new data").isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    private static SchemaCompatibilityType type(Schema reader, Schema writer) {
        return SchemaPipeline.canRead(reader, writer).getType();
    }
}
