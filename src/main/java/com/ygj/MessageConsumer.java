package com.ygj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by yangguojun on 2017/11/30.
 */
public class MessageConsumer extends Thread {

    private KafkaConsumer<String, String> consumer;

    private boolean running;

    private MessageHandler messageHandler;

    private long timeOut = 100;


    public MessageConsumer(Properties props, List<String> topic, MessageHandler messageHandler) {
        this.messageHandler = messageHandler;

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(topic);

        addShutdownHook();
    }

    public MessageConsumer(Properties props, Pattern pattern, MessageHandler messageHandler) {
        this.messageHandler = messageHandler;

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(pattern);
        addShutdownHook();
    }


    public MessageConsumer(Properties props, String topic, int partition, MessageHandler messageHandler) {
        this.messageHandler = messageHandler;

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        addShutdownHook();
    }


    @Override
    public void run() {
        running = true;
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeOut));
                Thread.sleep(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        messageHandler.handleMessage(record.key(), record.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        consumer.close();
    }


    public void stopRunning() {
        running = false;
    }


    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                running = false;
            }
        }));
    }


    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }

}
