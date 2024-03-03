package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsyncCustomCallback {

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // KafkaProducer configuration setting

        Properties props = new Properties();

        // bootstrap.servers
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        // value.serializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        try (KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props)) {

            for (int seq = 0; seq < 20; seq++) {

                Callback callback = new CustomCallback(seq);

                // ProducerRecord object creation
                ProducerRecord<Integer, String> producerRecord =
                        new ProducerRecord<>(topicName, seq, "hello world " + seq);

                // KafkaProducer message send
                kafkaProducer.send(producerRecord, callback);

            }
        }


    }

}
