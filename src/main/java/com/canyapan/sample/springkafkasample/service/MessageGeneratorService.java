package com.canyapan.sample.springkafkasample.service;

import com.canyapan.sample.springkafkasample.kafka.producer.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(value = "feature.kafka.message-generator.enabled", havingValue = "true")
public class MessageGeneratorService {

    private final KafkaProducerService kafkaProducerService;

    private final AtomicLong counter = new AtomicLong();

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.parallel.consumer.topic}")
    private String pTopic;

    @Scheduled(initialDelay = 2000L, fixedDelay = 100L)
    public void applicationReady() {
        long count = counter.incrementAndGet();
        kafkaProducerService.publish(topic, UUID.randomUUID().toString(), "Message " + count);
        kafkaProducerService.publish(pTopic, UUID.randomUUID().toString(), "Message " + count);

        if (count % 1000 == 0) {
            log.info("Produced {}", count);
        }
    }

}
