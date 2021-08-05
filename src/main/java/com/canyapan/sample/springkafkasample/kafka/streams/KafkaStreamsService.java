package com.canyapan.sample.springkafkasample.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsService {

    @Value("${kafka.topic}")
    private String inTopic;

    @Value("${kafka.topic2}")
    private String outTopic;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));
        stream.map(this::uppercaseValue)
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
        return stream;
    }

    private KeyValue<String, String> uppercaseValue(String key, String value) {
        log.info("stream processed: {}", value);
        return new KeyValue<>(key, value.toUpperCase());
    }

}
