package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class.getName());
    protected KafkaConsumer<K, V> kafkaConsumer;
    protected List<String> topics;

    private final OrderDBHandler orderDBHandler;

    public FileToDBConsumer(Properties consumerProps, List<String> topics,
                            OrderDBHandler orderDBHandler) {

        this.kafkaConsumer = new KafkaConsumer<>(consumerProps);
        this.topics = topics;
        this.orderDBHandler = orderDBHandler;
    }

    public void initConsumer() {

        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {

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

    private OrderDTO makeOrderDTO(ConsumerRecord<K, V> record) {

        String messageValue = (String) record.value();
        logger.info("###### messageValue:" + messageValue);

        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return new OrderDTO(tokens[0], tokens[1], tokens[2], tokens[3],
                tokens[4], tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));
    }


    private void processRecords(ConsumerRecords<K, V> records) {

        orderDBHandler.insertOrders(makeOrders(records));
    }

    private List<OrderDTO> makeOrders(ConsumerRecords<K, V> records) {

        List<OrderDTO> orders = new ArrayList<>();
        records.forEach(record -> orders.add(makeOrderDTO(record)));

        return orders;
    }


    public void pollConsumes(long durationMillis, String commitMode) {
        if (commitMode.equals("sync")) {
            pollCommitSync(durationMillis);
        } else {
            pollCommitAsync(durationMillis);
        }
    }

    private void pollCommitAsync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                logger.info("consumerRecords count:" + consumerRecords.count());
                if (consumerRecords.count() > 0) {
                    try {
                        processRecords(consumerRecords);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }

                this.kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }

    protected void pollCommitSync(long durationMillis) {
        try {
            while (true) {
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
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }

    protected void close() {

        this.kafkaConsumer.close();
        this.orderDBHandler.close();
    }

    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

        FileToDBConsumer<String, String> fileToDBConsumer = new FileToDBConsumer<>(props, List.of(topicName), orderDBHandler);
        fileToDBConsumer.initConsumer();

        String commitMode = "async";
        fileToDBConsumer.pollConsumes(1000, commitMode);

    }

}