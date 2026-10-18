package com.taca.shipment.event.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class ShipmentKafkaListenerConfiguration {
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> shipmentKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${shipment.kafka.consumer-dlq-topic:shipment.consumer.dlq.v1}") String dlqTopic,
            @Value("${shipment.kafka.consumer-retry-count:3}") int retries) {
        if (retries < 0 || retries > 10)
            throw new IllegalArgumentException("shipment.kafka.consumer-retry-count must be between 0 and 10");
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(dlqTopic, -1));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setHeadersFunction((record, exception) -> {
            var headers = new RecordHeaders();
            Header deliveryAttempt = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
            if (deliveryAttempt != null)
                headers.add("shipment_dlq_attempts", deliveryAttempt.value());
            headers.add("shipment_dlq_reason_code",
                    exception.getClass().getSimpleName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return headers;
        });
        var backOff = new ExponentialBackOffWithMaxRetries(retries);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(30_000L);
        var errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(PermanentShipmentMessageException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
