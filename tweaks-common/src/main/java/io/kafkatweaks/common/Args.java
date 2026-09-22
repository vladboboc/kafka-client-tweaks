package io.kafkatweaks.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Demo arguments in {@code key=value} form, e.g. {@code records=50000 linger.ms=20 compression=zstd}.
 * <p>
 * Every demo documents its keys; anything not recognised is still accepted so you can pass raw
 * Kafka client properties straight through with {@link #applyOverrides(java.util.Properties)}.
 */
public final class Args {

    private final Map<String, String> values = new LinkedHashMap<>();

    public static Args parse(String[] tokens, int from) {
        var args = new Args();
        for (int i = from; i < tokens.length; i++) {
            String token = tokens[i];
            int eq = token.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("expected key=value, got '" + token + "'");
            }
            args.values.put(token.substring(0, eq).trim(), token.substring(eq + 1).trim());
        }
        return args;
    }

    public boolean has(String key) {
        return values.containsKey(key);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    public String get(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return get(key).map(Integer::parseInt).orElse(defaultValue);
    }

    public long getLong(String key, long defaultValue) {
        return get(key).map(Long::parseLong).orElse(defaultValue);
    }

    public boolean getBool(String key, boolean defaultValue) {
        return get(key).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    public <E extends Enum<E>> E getEnum(String key, E defaultValue) {
        return get(key)
                .map(v -> Enum.valueOf(defaultValue.getDeclaringClass(), v.toUpperCase().replace('-', '_')))
                .orElse(defaultValue);
    }

    /**
     * Copies every argument whose key contains a dot into the given client properties, so
     * {@code linger.ms=20} or {@code fetch.min.bytes=1048576} on the command line override whatever
     * the demo configured. Keys without a dot are demo parameters, not Kafka configs.
     */
    public java.util.Properties applyOverrides(java.util.Properties props) {
        values.forEach((k, v) -> {
            if (k.indexOf('.') > 0) {
                props.put(k, v);
            }
        });
        return props;
    }

    public Map<String, String> asMap() {
        return Map.copyOf(values);
    }

    @Override
    public String toString() {
        return values.isEmpty() ? "(defaults)" : values.toString();
    }
}
