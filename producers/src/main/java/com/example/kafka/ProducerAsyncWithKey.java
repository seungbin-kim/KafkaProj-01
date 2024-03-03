package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncWithKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class);

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // KafkaProducer configuration setting

        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // org.apache.kafka.common.serialization.StringSerializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

            for (int seq = 0; seq < 20; seq++) {

                // ProducerRecord object creation
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, String.valueOf(seq), "hello world " + seq);

                // KafkaProducer message send
                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("\n" +
                                "##### record metadata received ##### \n" +
                                "topic: " + metadata.topic() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp());
                        return;
                    }

                    logger.error("exception error from broker ", exception);
                });

            }
        }


    }

}
