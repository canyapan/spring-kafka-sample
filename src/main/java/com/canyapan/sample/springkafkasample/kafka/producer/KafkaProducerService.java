package com.canyapan.sample.springkafkasample.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    public void publish(final String message) {

        try {
            final ListenableFuture<SendResult<String, String>> listenableFuture
                    = kafkaTemplate.send(topic, message);

            listenableFuture.addCallback(
                    result -> log.info("Published msg: {}", message),
                    cause -> log.error(String.format("Failed to publish msg: %s", message), cause));
        } catch (KafkaException e) {
            log.error(String.format("Failed to publish msg: %s", message), e);
        }

    }

}
