package com.canyapan.sample.springkafkasample.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    @KafkaListener(topics = {"${kafka.topic2}"})
    public void consume(final String message) {
        log.info("Received message: {}", message);
    }

}
