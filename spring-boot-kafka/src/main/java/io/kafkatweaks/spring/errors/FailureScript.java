package io.kafkatweaks.spring.errors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Chapter 18 scripts failures into record keys so that a run is repeatable and the tables can be read
 * without knowing the code. Grammar of a key:
 * <pre>
 *   ok-&lt;n&gt;         processed on the first attempt
 *   flaky&lt;k&gt;-&lt;n&gt;   throws a retryable exception on the first k attempts, then succeeds
 *   fatal-&lt;n&gt;      throws an exception the error handler is told never to retry
 *   poison-&lt;n&gt;     the value is not valid JSON: fails in the deserializer, before any listener
 * </pre>
 */
public final class FailureScript {

    public enum Kind { OK, FLAKY, FATAL, POISON }

    public record Plan(Kind kind, int failures) {

        /** Whether the listener should throw a retryable exception on this (1-based) attempt. */
        public boolean failsOn(int attempt) {
            return kind == Kind.FLAKY && attempt <= failures;
        }
    }

    private static final Pattern KEY = Pattern.compile("(ok|flaky(\\d+)|fatal|poison)-(\\d+)");

    private FailureScript() {
    }

    public static Plan parse(String key) {
        Matcher m = key == null ? null : KEY.matcher(key);
        if (m == null || !m.matches()) {
            throw new IllegalArgumentException("not a scripted key: " + key + " (expected ok-<n>, flaky<k>-<n>, fatal-<n> or poison-<n>)");
        }
        String kind = m.group(1);
        return switch (kind.charAt(0)) {
            case 'o' -> new Plan(Kind.OK, 0);
            case 'f' -> kind.startsWith("flaky") ? new Plan(Kind.FLAKY, Integer.parseInt(m.group(2))) : new Plan(Kind.FATAL, 0);
            default -> new Plan(Kind.POISON, 0);
        };
    }

    public static String describe(String key) {
        Plan plan = parse(key);
        return switch (plan.kind()) {
            case OK -> "succeeds";
            case FLAKY -> "fails " + plan.failures() + "x, then succeeds";
            case FATAL -> "not retryable";
            case POISON -> "undeserializable";
        };
    }
}
