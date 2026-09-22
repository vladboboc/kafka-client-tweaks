package io.kafkatweaks.common;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Test payloads. Two families on purpose:
 * <ul>
 *   <li>{@link #json(long, int)}: an order-like JSON document padded to a target size. Repetitive text
 *       with lots of structure, so it compresses 5–10x. This is what real event payloads look like.</li>
 *   <li>{@link #random(int)}: random bytes. Incompressible, so it shows what compression costs when it
 *       cannot help (CPU spent, {@code compression-rate-avg} near 1.0).</li>
 * </ul>
 */
public final class Payloads {

    private static final String[] CUSTOMERS = {"acme", "globex", "initech", "umbrella", "hooli", "vandelay", "wonka"};
    private static final String[] SKUS = {"SKU-1001", "SKU-1002", "SKU-2048", "SKU-3141", "SKU-4242", "SKU-8080"};
    private static final Random RANDOM = new Random(42);

    private Payloads() {
    }

    /** A deterministic order-ish JSON string of roughly {@code approxBytes} bytes. */
    public static String json(long seq, int approxBytes) {
        var sb = new StringBuilder(approxBytes + 64);
        sb.append("{\"orderId\":\"ORD-").append(seq)
          .append("\",\"customerId\":\"").append(CUSTOMERS[(int) (seq % CUSTOMERS.length)])
          .append("\",\"currency\":\"EUR\",\"status\":\"CREATED\",\"lines\":[");
        int line = 0;
        while (sb.length() < approxBytes - 16) {
            if (line > 0) {
                sb.append(',');
            }
            sb.append("{\"sku\":\"").append(SKUS[(int) ((seq + line) % SKUS.length)])
              .append("\",\"qty\":").append(1 + (line % 5))
              .append(",\"unitPrice\":").append(9.99 + line)
              .append(",\"note\":\"line item ").append(line).append(" of order ").append(seq).append("\"}");
            line++;
        }
        sb.append("]}");
        return sb.toString();
    }

    /** Incompressible random bytes rendered as a Latin-1 string so it can go through StringSerializer. */
    public static String random(int bytes) {
        var buf = new byte[bytes];
        RANDOM.nextBytes(buf);
        // Map every byte into the printable range so the string survives UTF-8 round trips at ~1 byte/char.
        for (int i = 0; i < buf.length; i++) {
            buf[i] = (byte) (33 + Math.floorMod(buf[i], 94));
        }
        return new String(buf, StandardCharsets.ISO_8859_1);
    }

    /** A key with bounded cardinality so partition/key demos get repeats: {@code customer-<n>}. */
    public static String key(long seq, int cardinality) {
        return "customer-" + (seq % cardinality);
    }

    public static int utf8Length(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }
}
