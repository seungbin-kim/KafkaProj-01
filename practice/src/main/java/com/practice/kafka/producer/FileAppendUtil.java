package com.practice.kafka.producer;

import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class FileAppendUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileAppendUtil.class);

    private static final Random random = new Random();

    private static final Faker faker = Faker.instance(Locale.KOREAN);

    private static final List<String> pizzaNames =
            List.of("포테이토 피자", "치즈 피자", "치즈 갈릭 피자", "슈퍼 슈프림", "페퍼로니 피자", "파인애플 피자");

    private static final List<String> pizzaShop =
            List.of("A001", "B001", "C001", "D001", "E001", "F001", "G001", "H001", "I001"
                    , "J001", "K001", "L001", "M001", "N001", "O001", "P001", "Q001");

    private static final String FILE_PATH = "/Users/seungbin/Desktop/git/KafkaProj-01/practice/src/main/resources/pizza_append.txt";

    private int orderSeq = 5000;

    private static String getRandomValueFromList(List<String> list) {
        int size = list.size();
        int index = random.nextInt(size);

        return list.get(index);
    }

    public static HashMap<String, String> produceMessage(int id) {

        String shopId = getRandomValueFromList(pizzaShop);
        String pizzaName = getRandomValueFromList(pizzaNames);

        String ordId = "ord" + id;
        String customerName = faker.name().fullName();
        String phoneNumber = faker.phoneNumber().phoneNumber();
        String address = faker.address().streetAddress();
        LocalDateTime now = LocalDateTime.now();

        String message = String.format(
                "order_id:%s, shop:%s, pizza_name:%s, customer_name:%s, phone_number:%s, address:%s, time:%s"
                , ordId, shopId, pizzaName, customerName, phoneNumber, address
                , now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN)));

        HashMap<String, String> messageMap = new HashMap<>();
        messageMap.put("key", shopId);
        messageMap.put("message", message);

        return messageMap;
    }

    public void writeMessage(String filePath) {
        try {
            File file = new File(filePath);
            if(!file.exists()) {
                file.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            PrintWriter printWriter = new PrintWriter(bufferedWriter);

            for(int i = 0; i < 50; i++) {
                HashMap<String, String> message = produceMessage(orderSeq++);
                printWriter.println(message.get("key") +"," + message.get("message"));
            }
            printWriter.close();

        } catch(IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args) {
        FileAppendUtil fileUtilAppend = new FileAppendUtil();

        // 100회 반복
        for(int i = 0; i < 100; i++) {
            //50 라인의 주문 문자열을 write
            fileUtilAppend.writeMessage(FILE_PATH);
            System.out.println("###### iteration:" + i + " file write is done");
            try {
                Thread.sleep(500);
            }catch(InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
