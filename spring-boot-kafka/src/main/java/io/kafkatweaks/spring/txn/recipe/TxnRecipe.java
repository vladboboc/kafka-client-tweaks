package io.kafkatweaks.spring.txn.recipe;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.List;

/**
 * Chapter 19 · Transactions the Spring way. The switch is ONE property, {@code spring.kafka.producer.transaction-id-prefix}
 * (application-spring-transactions.yml): the producer factory becomes transactional, Boot auto-configures a
 * {@code KafkaTransactionManager} and wires it into the listener container factory. What is left for code is here, in
 * {@link OrderTransfer} ({@code @Transactional}) and in {@link UppercaseProcessor} (exactly-once listeners).
 * <p>
 * Measured by the {@code spring-transactions} demo (docs/19-spring-transactions.md): an aborted and a committed
 * transaction of 10 records each; read_committed saw 10, read_uncommitted also the aborted ones.
 */
@Configuration(proxyBeanMethods = false)
@Profile("spring-transactions")
@EnableTransactionManagement   // Boot's spring-boot-tx module does this when present; spelled out so @Transactional always works
public class TxnRecipe {

    /**
     * Several records, any number of partitions, one transaction: all visible to read_committed consumers or none (the
     * callback threw). No listener and no {@code @Transactional} needed around it.
     */
    public static <K, V> void sendAll(KafkaTemplate<K, V> template, List<ProducerRecord<K, V>> records) {
        template.executeInTransaction(ops -> {
            records.forEach(ops::send);
            return null;
        });
    }

    /**
     * A transactional template refuses a plain {@code send()} outside a transaction ({@code IllegalStateException}). A
     * second template on the SAME factory may be allowed to: the factory then hands out a non-transactional producer
     * for it. Not a bean: a second {@code KafkaTemplate} bean would switch Boot's auto-configured one off.
     */
    public static <K, V> KafkaTemplate<K, V> nonTransactionalTemplate(ProducerFactory<K, V> transactionalFactory) {
        var template = new KafkaTemplate<>(transactionalFactory);
        template.setAllowNonTransactional(true);
        return template;
    }
}
