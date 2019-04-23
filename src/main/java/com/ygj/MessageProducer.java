package com.ygj;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by yangguojun on 2017/11/28.
 */
public class MessageProducer {

    private Properties properties;

    private KafkaProducer<String, String> messageProducer;

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    public MessageProducer(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("配置未初始化");
        }

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.properties = properties;

        this.messageProducer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (messageProducer != null) {
                    messageProducer.close();
                }
            }
        }));
    }


    public void sendMessage(String topic, int partition, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, key, message);
        this.messageProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                completion(metadata, e);
            }
        });
    }


    public void sendMessage(String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);
        this.messageProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                completion(metadata, e);
            }
        });
    }


    private void completion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            log.error("kafka producer send message error", e);
        } else {
            log.info("The offset of the record we just sent is: {}, and partition is: {} ", metadata.offset(), metadata.partition());
        }
    }

    public void close(){
        this.properties = null;
        if (messageProducer != null) {
            messageProducer.close();
        }
    }
}
