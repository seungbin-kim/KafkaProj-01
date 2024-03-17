package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerCommit {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerCommit.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_03");

        // manual commit
        String enableAutoCommit = "false";
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        boolean autoCommit = Boolean.parseBoolean(enableAutoCommit);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        // main thread
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }

        }));

//        pollAutoCommit(kafkaConsumer, autoCommit);
//        pollManualCommitSync(kafkaConsumer, autoCommit);
        pollManualCommitAsync(kafkaConsumer, autoCommit);
    }

    private static void pollManualCommitAsync(KafkaConsumer<String, String> kafkaConsumer,
                                              boolean autoCommit) {

        pollWhileTemplate(kafkaConsumer, autoCommit, false);
    }

    private static void pollManualCommitSync(KafkaConsumer<String, String> kafkaConsumer,
                                             boolean autoCommit) {

        pollWhileTemplate(kafkaConsumer, autoCommit, true);
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer,
                                       boolean autoCommit) {

        pollWhileTemplate(kafkaConsumer, autoCommit, true);
    }

    private static void pollWhileTemplate(KafkaConsumer<String, String> kafkaConsumer,
                                          boolean autoCommit, boolean sync) {
        int loopCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                logger.info("######### loopCount: {} consumerRecords count:{}", loopCount++, records.count());

                for (ConsumerRecord<String, String> record : records) {

                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                if (autoCommit) {
                    try {
                        logger.info("main thread is sleeping {} ms during while loop", 10000);
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                } else if (sync) {
                    try {
                        if (records.count() > 0) {
                            kafkaConsumer.commitSync();
                            logger.info("commit sync has been called");
                        }
                    } catch (CommitFailedException e) { // 여러번 commit 재시도 하다가 더이상 재시도 안할 때 발생
                        logger.error(e.getMessage());
                    }
                } else { // Async
//                    if (records.isEmpty()) continue;
                    kafkaConsumer.commitAsync((offsets, exception) -> {
                        if (exception != null) { // 오류 발생시 null 이 아닌 값이 들어옴
                            logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                            return;
                        }

                        logger.info("commit async. offsets: {}", offsets);
                    });
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                // auto commit 이 아니라면 close() 전 manual commit sync 해주자
                if (!autoCommit) {
                    kafkaConsumer.commitSync();
                    logger.info("consumer commit sync");
                }
            } catch (CommitFailedException e) {
                logger.error(e.getMessage());
            }

            kafkaConsumer.close();
            logger.info("finally consumer is closing");
        }
    }

}
