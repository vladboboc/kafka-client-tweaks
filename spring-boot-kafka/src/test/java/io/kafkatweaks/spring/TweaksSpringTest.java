package io.kafkatweaks.spring;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code @SpringBootTest} with the {@code test} profile: KafkaAdmin does not try to create the RF-3 demo topics
 * (see application-test.yml), listeners stay stopped (application.yml), and no demo profile is active, so the
 * context is the shared wiring only. Tests that need a broker add {@code @EmbeddedKafka}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@ActiveProfiles("test")
public @interface TweaksSpringTest {
}
