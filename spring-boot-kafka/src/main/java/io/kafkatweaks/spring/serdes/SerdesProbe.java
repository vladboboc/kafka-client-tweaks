package io.kafkatweaks.spring.serdes;

import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chapter 21's measurement inside the recipe listeners ({@code recipe/SerdesListeners}): per listener, how many records
 * it saw, the class of the first value, the {@code __TypeId__} header it came with and a sample, so the demo can put the
 * five deserialization set-ups side by side.
 */
@Component
@Profile("spring-serdes")
public class SerdesProbe {

    public static final class Received {
        private final AtomicLong count = new AtomicLong();
        private volatile String valueClass = "-";
        private volatile String typeIdHeader = "-";
        private volatile String sample = "-";

        public long count() {
            return count.get();
        }

        public String valueClass() {
            return valueClass;
        }

        public String typeIdHeader() {
            return typeIdHeader;
        }

        public String sample() {
            return sample;
        }
    }

    private final Map<String, Received> received = new ConcurrentHashMap<>();

    public Received received(String listenerId) {
        return received.computeIfAbsent(listenerId, k -> new Received());
    }

    /** A record's value as the listener got it, with the record headers. */
    public void note(String listenerId, Object value, Iterable<Header> headers) {
        Received r = received(listenerId);
        if (r.count.incrementAndGet() == 1) {
            String typeId = "(none)";
            for (Header h : headers) {
                if (h.key().equals(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)) {
                    typeId = new String(h.value(), StandardCharsets.UTF_8);
                }
            }
            first(r, value, typeId);
        }
    }

    /** Same, for a listener that got the {@code __TypeId__} header as a method parameter (null when absent). */
    public void note(String listenerId, Object value, String typeIdHeader) {
        Received r = received(listenerId);
        if (r.count.incrementAndGet() == 1) {
            first(r, value, typeIdHeader == null ? "(none)" : typeIdHeader);
        }
    }

    private static void first(Received r, Object value, String typeIdHeader) {
        r.valueClass = value == null ? "null" : value.getClass().getName();
        r.sample = value == null ? "null" : abbreviate(value.toString(), 70);
        r.typeIdHeader = typeIdHeader;
    }

    static String abbreviate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }
}
