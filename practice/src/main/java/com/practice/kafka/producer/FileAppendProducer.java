package com.practice.kafka.producer;

import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    private static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class);

    private static final String FILE_PATH = "/Users/seungbin/Desktop/git/KafkaProj-01/practice/src/main/resources/pizza_append.txt";

    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 메세지를 보내기 위해 구현한 핸들러
        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, false);

        // 파일
        File file = new File(FILE_PATH);

        // 파일 모니터링을 위한 쓰레드 생성
        FileEventSource fileEventSource = new FileEventSource(fileEventHandler, 1000, file);
        Thread fileEventSourceThread = new Thread(fileEventSource);

        // 쓰레드 시작
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }

}
