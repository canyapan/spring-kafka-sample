package com.canyapan.sample.springkafkasample.kafka.consumer;

import com.canyapan.sample.springkafkasample.service.DataProcService;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@ConditionalOnProperty(value = "feature.kafka.parallel-consumer.enabled", havingValue = "true")
public class KafkaParallelConsumerService {

    private final String topic;
    private final ParallelStreamProcessor<String, String> processor;
    private final DataProcService service;

    public KafkaParallelConsumerService(
            @Value("${kafka.parallel.consumer.topic}") String topic,
            @Value("${kafka.parallel.consumer.concurrency:100}") int concurrency,
            DefaultKafkaConsumerFactory<String, String> consumerFactory,
            MeterRegistry meterRegistry, DataProcService service) {
        this.topic = topic;
        this.service = service;

        Map<String, Object> configs = new HashMap<>(consumerFactory.getConfigurationProperties());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configs);

        ParallelConsumerOptions<String, String> parallelConsumerOptions = ParallelConsumerOptions
                .<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .maxConcurrency(concurrency)
                .consumer(kafkaConsumer)
                .meterRegistry(meterRegistry)
                .build();

        this.processor = ParallelStreamProcessor.createEosStreamProcessor(parallelConsumerOptions);
    }

    @Scheduled(initialDelayString = "1s")
    public void consume() {
        try {
            processor.subscribe(List.of(topic));
            processor.poll(ctx -> {
                ctx.forEach(record -> {
                    String key = record.key();
                    String message = record.value();
                    int partition = record.partition();
                    long offset = record.offset();

                    service.process(key, message);

                    log.info("Concurrently processed a key: {}, value: {}, partition: {}, offset: {}", key, message, partition, offset);
                });
            });
            log.info("Started parallel consumer");
        } catch (Exception e) {
            log.error("Starting parallel consumer failed", e);
        }
    }

    @PreDestroy
    public void end() {
        try {
            processor.close(DrainingCloseable.DrainingMode.DONT_DRAIN);
        } catch (Exception e) {
            log.error("Closing parallel consumer failed", e);
        }
    }

}
