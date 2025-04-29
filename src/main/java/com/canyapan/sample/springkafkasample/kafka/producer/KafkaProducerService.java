package com.canyapan.sample.springkafkasample.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;


    public void publish(final String topic, final String key, final String message) {

        try {
            final CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

            future.whenCompleteAsync((result, cause) -> {
                if (null != cause) {
                    log.error("Failed to publish key: %s msg: %s".formatted(key, message), cause);
                    return;
                }

                log.debug("Published key: {}. msg: {}, result: {}", key, message, result);
            });
        } catch (KafkaException e) {
            log.error(String.format("Failed to publish msg: %s", message), e);
        }

    }

}
