package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {
    private static final Logger logger = LoggerFactory.getLogger(FileProducer.class);

    private static final String TOPIC_NAME = "file-topic";

    private static final String FILE_PATH = "/Users/seungbin/Desktop/git/KafkaProj-01/practice/src/main/resources/pizza_sample.txt";

    private static final String DELIMITER_REGEX = "\\s*,\\s*";

    public static void main(String[] args) {

        // KafkaProducer configuration setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // org.apache.kafka.common.serialization.StringSerializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

            sendFileMessages(kafkaProducer);

        }


    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer) {

        try (FileReader fileReader = new FileReader(FILE_PATH);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            String line;
            while ((line = bufferedReader.readLine()) != null) {

//                String[] tokens = line.split(DELIMITER_REGEX);
                String[] tokens = line.split(DELIMITER_REGEX, 2);

                String key = tokens[0];

//                String value = String.join(",", Arrays.copyOfRange(tokens, 1, tokens.length));
                String value = tokens[1];

                sendMessage(kafkaProducer, key, value);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                    String key,
                                    String value) {

        logger.info("key: {} | value: {}", key, value);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);

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
