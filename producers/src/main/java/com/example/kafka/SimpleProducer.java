package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting

        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // org.apache.kafka.common.serialization.StringSerializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

            // ProducerRecord object creation
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

            // KafkaProducer message send
            kafkaProducer.send(producerRecord);

            kafkaProducer.flush();
        }
    }

}
