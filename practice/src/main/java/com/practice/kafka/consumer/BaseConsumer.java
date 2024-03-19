package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.practice.kafka.consumer.BaseConsumer.CommitMode.*;

public class BaseConsumer<K extends Serializable, V extends Serializable> {

    public enum CommitMode {
        SYNC,
        ASYNC
    }

    public static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class.getName());

    private final KafkaConsumer<K, V> kafkaConsumer;

    private final List<String> topics;

    public BaseConsumer(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<>(consumerProps);
        this.topics = topics;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime();
    }

    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    public void pollConsumes(long durationMillis, CommitMode commitMode) {
        try {
            while (true) {
                if (SYNC.equals(commitMode)) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void shutdownHookToRuntime() {

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }));

    }

    private void processRecord(ConsumerRecord<K, V> record) {

        logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {

        records.forEach(this::processRecord);
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException {

        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);

        kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                return;
            }

            logger.info("commit async. offsets: {}", offsets);
        });
    }

    private void pollCommitSync(long durationMillis) throws WakeupException {

        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if (consumerRecords.count() > 0) {
                this.kafkaConsumer.commitSync();
                logger.info("commit sync has been called");
            }
        } catch (CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(props, List.of(topicName));
        baseConsumer.initConsumer();

        baseConsumer.pollConsumes(100, SYNC);
        baseConsumer.closeConsumer();

    }


}