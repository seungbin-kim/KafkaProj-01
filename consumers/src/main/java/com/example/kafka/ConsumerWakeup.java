package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        // 기본적인 설정
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01-static");
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        // main thread
        Thread mainThread = Thread.currentThread();

        // 별도의 thread 를 통해서 프로그램 종료시 kafkaConsumer wakeup() 호출 -> poll() 수행시 예외발생
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join(); // 메인쓰레드 정상종료를 대기한다.
            } catch (InterruptedException e) {e.printStackTrace();}

        }));

        try {
            while (true) {
                // 브로커나 Consumer 내부 Queue에 데이터가 있다면 바로 데이터 반환
                // 없다면 최대 Duration 동안 Fetch를 계속 Borker에 수행
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            kafkaConsumer.close();
        }

    }

}
