package az.cbar.hub.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    /**
     * Retries a failing record 3 times, 1 s apart, then logs and skips it so one bad
     * message does not block its partition forever. Boot wires this bean into the
     * listener container factory automatically. Next step: publish skipped records to a DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(
                (record, ex) -> log.error("Giving up on {}-{} offset={} key={}: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage()),
                new FixedBackOff(1000L, 3));
    }
}
