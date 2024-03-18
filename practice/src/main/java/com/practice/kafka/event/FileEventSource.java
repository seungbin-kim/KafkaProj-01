package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private final EventHandler eventHandler;

    private final long updateInterval;

    private final File file;

    private long filePointer;

    private boolean keepRunning = true;

    public FileEventSource(EventHandler eventHandler, long updateInterval, File file) {
        this.eventHandler = eventHandler;
        this.updateInterval = updateInterval;
        this.file = file;
    }

    @Override
    public void run() {

        try {
            while (keepRunning) {
                Thread.sleep(updateInterval);

                // 파일 크기 계산
                long length = file.length();

                if (length < filePointer) {
                    logger.info("file was reset as filePointer is longer than file length");
                    filePointer = length;
                } else if (length > filePointer) {
                    readAppendedAndSend();
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private void readAppendedAndSend() throws IOException, ExecutionException, InterruptedException {

        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(filePointer);

        String line;
        while ((line = raf.readLine()) != null) { // 파일 마지막까지 한 라인씩 읽음
            sendMessage(line);
        }

        // 읽은 마지막 위치로 재 설정
        filePointer = raf.getFilePointer();

        raf.close();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {

        String[] tokens = line.split(",", 2);

        String key = tokens[0];
        String value = tokens[1];

        MessageEvent messageEvent = new MessageEvent(key, value);
        eventHandler.onMessage(messageEvent);
    }

}
