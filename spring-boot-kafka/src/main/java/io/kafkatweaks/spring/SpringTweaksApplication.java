package io.kafkatweaks.spring;

import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

/**
 * One Spring Boot application, one demo per run. The first plain argument names the demo and becomes the active
 * profile, so {@code application-<demo>.yml} (that chapter's {@code spring.kafka.*} knobs) and the chapter's
 * {@code @Profile("<demo>")} beans are the only chapter-specific things loaded:
 * <pre>
 *   ./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run -Dspring-boot.run.arguments="spring-template records=20000"
 * </pre>
 * The remaining {@code key=value} arguments are the demo's own knobs ({@link io.kafkatweaks.common.Args}). Boot-style
 * {@code --spring.kafka.producer.properties.linger.ms=50} options are ordinary Spring properties and can be added
 * anywhere on the line: that is the Spring way of tweaking a client without touching YAML. Without a known demo name
 * the application runs with the {@link Catalogue#PROFILE catalogue} profile, which lists the demos.
 */
@SpringBootApplication
public class SpringTweaksApplication {

    public static void main(String[] args) {
        List<String> plain = new DefaultApplicationArguments(args).getNonOptionArgs();
        String profile = plain.isEmpty() || Catalogue.find(plain.getFirst()).isEmpty() ? Catalogue.PROFILE : plain.getFirst();
        ConfigurableApplicationContext context = new SpringApplicationBuilder(SpringTweaksApplication.class)
                .profiles(profile)
                .run(args);
        // The demo body ran as an ApplicationRunner inside run(). Closing the context stops the listener containers
        // and closes the producers; the exit code is whatever the demo left behind.
        System.exit(SpringApplication.exit(context, DemoSupport::exitCode));
    }
}
