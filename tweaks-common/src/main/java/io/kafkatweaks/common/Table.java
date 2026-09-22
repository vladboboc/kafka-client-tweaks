package io.kafkatweaks.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Minimal fixed-width table printer. Numbers are right-aligned and humanised
 * ({@code 1234567 -> 1.23M}), everything else is left-aligned.
 */
public final class Table {

    private final String[] header;
    private final List<String[]> rows = new ArrayList<>();

    public Table(String... header) {
        this.header = header;
    }

    public Table row(Object... cells) {
        if (cells.length != header.length) {
            throw new IllegalArgumentException("expected " + header.length + " cells, got " + cells.length);
        }
        var out = new String[cells.length];
        for (int i = 0; i < cells.length; i++) {
            out[i] = format(cells[i]);
        }
        rows.add(out);
        return this;
    }

    public void print() {
        print("");
    }

    public void print(String title) {
        var sb = new StringBuilder();
        if (!title.isBlank()) {
            sb.append('\n').append(title).append('\n');
        }
        int[] widths = new int[header.length];
        for (int i = 0; i < header.length; i++) {
            widths[i] = header[i].length();
            for (var row : rows) {
                widths[i] = Math.max(widths[i], row[i].length());
            }
        }
        sb.append(line(header, widths, true));
        sb.append(separator(widths));
        for (var row : rows) {
            sb.append(line(row, widths, false));
        }
        System.out.print(sb);
        System.out.flush();
    }

    private static String line(String[] cells, int[] widths, boolean isHeader) {
        var sb = new StringBuilder("| ");
        for (int i = 0; i < cells.length; i++) {
            boolean numeric = !isHeader && isNumeric(cells[i]);
            String cell = numeric
                    ? " ".repeat(widths[i] - cells[i].length()) + cells[i]
                    : cells[i] + " ".repeat(widths[i] - cells[i].length());
            sb.append(cell).append(" | ");
        }
        return sb.append('\n').toString();
    }

    private static String separator(int[] widths) {
        var sb = new StringBuilder("|");
        for (int w : widths) {
            sb.append("-".repeat(w + 2)).append('|');
        }
        return sb.append('\n').toString();
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) {
            return false;
        }
        char first = s.charAt(0);
        return Character.isDigit(first) || first == '-' || first == '+';
    }

    /** Humanises numbers; leaves everything else as {@code String.valueOf}. */
    public static String format(Object o) {
        return switch (o) {
            case null -> "-";
            case Double d -> formatNumber(d);
            case Float f -> formatNumber(f);
            case Long l -> formatNumber(l);
            case Integer i -> formatNumber(i);
            default -> String.valueOf(o);
        };
    }

    public static String formatNumber(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "inf" : "-inf";
        }
        // Locale.ROOT, not the JVM's default: on a machine whose format locale uses a decimal comma the tables
        // would not match the ones printed in the chapters (and the unit tests would fail on a clean checkout).
        double abs = Math.abs(v);
        if (abs >= 1_000_000_000d) {
            return String.format(Locale.ROOT, "%.2fG", v / 1_000_000_000d);
        }
        if (abs >= 1_000_000d) {
            return String.format(Locale.ROOT, "%.2fM", v / 1_000_000d);
        }
        if (abs >= 10_000d) {
            return String.format(Locale.ROOT, "%.1fK", v / 1_000d);
        }
        if (abs >= 100d || v == Math.rint(v)) {
            return String.format(Locale.ROOT, "%.0f", v);
        }
        return String.format(Locale.ROOT, "%.2f", v);
    }
}
