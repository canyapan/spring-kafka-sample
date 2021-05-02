package com.canyapan.sample.springkafkasample.service;

import com.canyapan.sample.springkafkasample.kafka.producer.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageGeneratorService {

    private final KafkaProducerService kafkaProducerService;

    private final AtomicLong counter = new AtomicLong();

    @Scheduled(initialDelay = 10000L, fixedDelay = 1000L)
    public void applicationReady() {
        kafkaProducerService.publish("Message " + counter.incrementAndGet());
    }

}
