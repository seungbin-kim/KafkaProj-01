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

public class ConsumerWakeupV2 {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_02");

        // 60초만 poll 기다리게끔 -> 60초간 poll 없다면 리밸런싱 일어난다.
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

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

        int loopCount = 0;

        try {
            while (true) {
                // 브로커나 Consumer 내부 Queue에 데이터가 있다면 바로 데이터 반환
                // 없다면 최대 Duration 동안 Fetch를 계속 Borker에 수행
                // 첫번째 poll() 은 HeartBeat Thread가 만들어지고 브로커와 인사한다.(레코드를 가져오지 않는다.)
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                logger.info("######### loopCount: {} consumerRecords count:{}", loopCount++, records.count());

                for (ConsumerRecord<String, String> record : records) {

                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    // 매 루프마다 쉬는시간을 늘린다.
                    int sleepTime = loopCount * 10000;
                    logger.info("main thread is sleeping {} ms during while loop", sleepTime);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            kafkaConsumer.close();
        }

    }

}
