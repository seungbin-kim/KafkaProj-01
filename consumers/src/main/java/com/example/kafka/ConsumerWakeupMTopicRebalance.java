package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupMTopicRebalance {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupMTopicRebalance.class);

    public static void main(String[] args) {

        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");

        // 파티션 할당 전략
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 2개의 토픽 subscribe
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // main thread
        Thread mainThread = Thread.currentThread();

        // 별도의 thread 를 통해서 프로그램 종료시 kafkaConsumer wakeup() 호출 -> poll() 수행시 예외발생
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join(); // 메인쓰레드 정상종료를 대기한다.
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic:{}, record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.topic(), record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            kafkaConsumer.close();
        }

    }

}
