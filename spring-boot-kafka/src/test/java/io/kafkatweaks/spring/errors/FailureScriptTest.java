package io.kafkatweaks.spring.errors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailureScriptTest {

    @Test
    void parsesEveryKind() {
        assertThat(FailureScript.parse("ok-1")).isEqualTo(new FailureScript.Plan(FailureScript.Kind.OK, 0));
        assertThat(FailureScript.parse("flaky2-7")).isEqualTo(new FailureScript.Plan(FailureScript.Kind.FLAKY, 2));
        assertThat(FailureScript.parse("fatal-3")).isEqualTo(new FailureScript.Plan(FailureScript.Kind.FATAL, 0));
        assertThat(FailureScript.parse("poison-9")).isEqualTo(new FailureScript.Plan(FailureScript.Kind.POISON, 0));
    }

    @Test
    void flakyFailsExactlyKTimes() {
        FailureScript.Plan plan = FailureScript.parse("flaky2-1");
        assertThat(plan.failsOn(1)).isTrue();
        assertThat(plan.failsOn(2)).isTrue();
        assertThat(plan.failsOn(3)).isFalse();
        assertThat(FailureScript.parse("ok-1").failsOn(1)).isFalse();
        assertThat(FailureScript.parse("fatal-1").failsOn(1)).isFalse();   // fatal is not "retryable failing", the listener throws unconditionally
    }

    @Test
    void rejectsUnscriptedKeys() {
        assertThatThrownBy(() -> FailureScript.parse("customer-1")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a scripted key");
        assertThatThrownBy(() -> FailureScript.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void describesForTheTables() {
        assertThat(FailureScript.describe("flaky3-1")).isEqualTo("fails 3x, then succeeds");
        assertThat(FailureScript.describe("poison-2")).isEqualTo("undeserializable");
    }
}
