package io.kafkatweaks.common;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArgsTest {

    @Test
    void parsesKeyValuePairsAfterTheDemoName() {
        Args args = Args.parse(new String[] {"producer-batching", "records=50000", "payload=random", "linger.ms=20"}, 1);

        assertThat(args.getLong("records", 0)).isEqualTo(50_000);
        assertThat(args.get("payload", "json")).isEqualTo("random");
        assertThat(args.has("linger.ms")).isTrue();
        assertThat(args.getInt("missing", 7)).isEqualTo(7);
    }

    @Test
    void dottedKeysBecomeClientPropertiesAndPlainKeysDoNot() {
        Args args = Args.parse(new String[] {"records=10", "linger.ms=20", "fetch.min.bytes=1024"}, 0);
        Properties props = new Properties();
        props.put("linger.ms", "5");

        args.applyOverrides(props);

        assertThat(props).containsEntry("linger.ms", "20").containsEntry("fetch.min.bytes", "1024").doesNotContainKey("records");
    }

    @Test
    void enumsAreCaseInsensitiveAndAcceptDashes() {
        Args args = Args.parse(new String[] {"payload=Random"}, 0);
        assertThat(args.getEnum("payload", Workload.Payload.JSON))
                .isEqualTo(Workload.Payload.RANDOM);
    }

    @Test
    void rejectsTokensWithoutEquals() {
        assertThatThrownBy(() -> Args.parse(new String[] {"records"}, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key=value");
    }
}
