package io.kafkatweaks.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableTest {

    @Test
    void humanisesNumbers() {
        assertThat(Table.formatNumber(0)).isEqualTo("0");
        assertThat(Table.formatNumber(42)).isEqualTo("42");
        assertThat(Table.formatNumber(3.14159)).isEqualTo("3.14");
        assertThat(Table.formatNumber(999.6)).isEqualTo("1000");
        assertThat(Table.formatNumber(16384)).isEqualTo("16.4K");
        assertThat(Table.formatNumber(1_500_000)).isEqualTo("1.50M");
        assertThat(Table.formatNumber(2_000_000_000d)).isEqualTo("2.00G");
        assertThat(Table.formatNumber(Double.NaN)).isEqualTo("NaN");
    }

    @Test
    void formatsCellsByType() {
        assertThat(Table.format(null)).isEqualTo("-");
        assertThat(Table.format(2048L)).isEqualTo("2048");
        assertThat(Table.format(0.5)).isEqualTo("0.50");
        assertThat(Table.format("zstd")).isEqualTo("zstd");
    }

    @Test
    void rejectsRowsOfTheWrongWidth() {
        assertThatThrownBy(() -> new Table("a", "b").row(1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
