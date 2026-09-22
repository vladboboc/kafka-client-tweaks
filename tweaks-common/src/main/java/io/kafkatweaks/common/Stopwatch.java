package io.kafkatweaks.common;

import java.time.Duration;

/** Wall-clock timer for the demos; {@link #rate(long)} turns a count into a per-second figure. */
public final class Stopwatch {

    private final long startNanos = System.nanoTime();

    public static Stopwatch start() {
        return new Stopwatch();
    }

    public Duration elapsed() {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    public double elapsedMillis() {
        return (System.nanoTime() - startNanos) / 1_000_000d;
    }

    public double elapsedSeconds() {
        return (System.nanoTime() - startNanos) / 1_000_000_000d;
    }

    /** Events per second since start. */
    public double rate(long count) {
        double s = elapsedSeconds();
        return s == 0 ? Double.NaN : count / s;
    }

    @Override
    public String toString() {
        return "%.0f ms".formatted(elapsedMillis());
    }
}
