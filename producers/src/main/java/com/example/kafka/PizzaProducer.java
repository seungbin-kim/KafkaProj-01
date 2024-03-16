package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class PizzaProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);

    private static final String TOPIC_NAME = "pizza-topic";

    public static void main(String[] args) {

        // KafkaProducer configuration setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // batch setting
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        // 전송/재전송 시도 관련
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");

        // idempotence
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        // KafkaProducer object creation
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

            sendPizzaMessage(kafkaProducer,
                    -1,
                    1000,
                    100,
                    3000,
                    false);
        }
    }

    private static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                         int iterationCount,
                                         int intervalCount,
                                         int intervalMillis,
                                         int interIntervalMillis,
                                         boolean sync) {

        Stream.iterate(0, seq -> seq != iterationCount, seq -> seq + 1)
                .forEach(seq -> {

                    HashMap<String, String> pizzaMessage = PizzaMessageFactory.produceMessage(seq);

                    sendMessage(kafkaProducer, pizzaMessage, sync);

                    // IntervalCount 만큼 돌때마다 IntervalMillis 만큼 쉰다.
                    if (intervalCount > 0 && (seq % intervalCount == 0)) {
                        try {
                            logger.info("########## IntervalCount: {} IntervalMillis: {} ##########", intervalCount, intervalMillis);
                            Thread.sleep(intervalMillis);
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }

                    // 매번 interIntervalMillis 만큼 쉰다.
                    if (interIntervalMillis > 0) {
                        try {
                            logger.info("@@@@@ interIntervalMillis: {} @@@@@", interIntervalMillis);
                            Thread.sleep(interIntervalMillis);
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                    HashMap<String, String> pizzaMessage,
                                    boolean sync) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME,
                pizzaMessage.get("key"),
                pizzaMessage.get("message"));

        // 비동기 전송
        if (!sync) {
            // send() 메서드는 Record Accumulator 에 레코드 적재후 바로 반환됨.
            // 본질적으로 비동기 이다.
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message:{}, partition:{}, offset:{}",
                            pizzaMessage.get("key"),
                            metadata.partition(),
                            metadata.offset());

                    return;
                }

                logger.error("exception error from broker ", exception);
            });

            return;
        }

        // 동기 전송
        try {
            RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
            logger.info("sync message:{}, partition:{}, offset:{}",
                    pizzaMessage.get("key"),
                    metadata.partition(),
                    metadata.offset());

        } catch (InterruptedException | ExecutionException e) {
            logger.error("exception error from broker ", e);
            throw new RuntimeException(e);
        }
    }

}
