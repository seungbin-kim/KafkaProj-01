package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private static final String TOPIC = "file-topic";

    private final KafkaProducer<String, String> kafkaProducer;

    private final boolean sync;


    public FileEventHandler(KafkaProducer<String, String> kafkaProducer,
                            boolean sync) {

        this.kafkaProducer = kafkaProducer;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                TOPIC,
                messageEvent.getKey(),
                messageEvent.getValue());

        // 동기
        if (sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();

            logger.info("\n" +
                    "##### record metadata received ##### \n" +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset() + "\n" +
                    "timestamp: " + recordMetadata.timestamp());

            return;
        }

        // 비동기
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n" +
                        "##### record metadata received ##### \n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp());
                return;
            }

            logger.error("exception error from broker ", exception);
        });
    }

}
