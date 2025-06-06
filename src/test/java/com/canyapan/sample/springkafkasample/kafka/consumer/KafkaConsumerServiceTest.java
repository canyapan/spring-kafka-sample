package com.canyapan.sample.springkafkasample.kafka.consumer;

import com.canyapan.sample.springkafkasample.service.DataProcService;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = {"${kafka.topic}"})
@TestPropertySource(properties = {
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "feature.kafka.consumer.enabled=true"})
class KafkaConsumerServiceTest {

    @Value("${kafka.topic}")
    private String topic;

    @MockitoBean
    private DataProcService serviceMock;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void init() {
        final Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        final ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer());

        kafkaTemplate = new KafkaTemplate<>(producerFactory, true);

        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void destroy() {
        kafkaListenerEndpointRegistry.stop();
    }

    @Test
    void shouldConsumeMessages() {
        final String key = UUID.randomUUID().toString();
        final String message = "Test message";

        kafkaTemplate.send(topic, key, message);

        verify(serviceMock, timeout(10000))
                .process(key, message);
    }
}