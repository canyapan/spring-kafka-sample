package com.canyapan.sample.springkafkasample.kafka.consumer;

import com.canyapan.sample.springkafkasample.service.DataProcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(value = "feature.kafka.consumer.enabled", havingValue = "true")
public class KafkaConsumerService {

    private final DataProcService service;

    public KafkaConsumerService(DataProcService service) {
        this.service = service;
    }

    @KafkaListener(topics = {"${kafka.topic}"}, concurrency = "${kafka.concurrency:2}")
    public void consume(
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) final int partition,
            @Header(value = KafkaHeaders.OFFSET) final long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY) final String key,
            @Payload final String message) {

        service.process(key, message);
        log.info("Processed a key: {}, value: {}, partition: {}, offset: {}", key, message, partition, offset);
    }

}
