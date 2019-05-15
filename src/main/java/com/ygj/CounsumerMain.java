package com.ygj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CounsumerMain {

    private static final Logger log = LoggerFactory.getLogger(CounsumerMain.class);

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";


    public static void main(String[] args) throws Exception {
        messageSASLConsumerTest();
    }


    /**
     * 外网接收消息
     */
    private static void messageSASL_SSLConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
        props.put("ssl.truststore.location", "D:\\Documents\\Downloads\\certificates_test\\certificates_test\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
        final Map<String, Integer> countMap = new HashMap();
        countMap.put("key", 0);
        MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(Constants.TOPIC_NAME), new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
                countMap.put("key", countMap.get("key") + 1);
                System.out.println(countMap.get("key"));
                System.out.println(message);
            }
        });
        consumer.start();

        while (true) {
        }
    }

    /**
     * 内网接收消息
     */
    private static void messageSASLConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-0021232358");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
        List<String> list = new ArrayList<>();
        list.add("topicName");
        MessageConsumer consumer = new MessageConsumer(props, list, new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
                System.out.println(new Date(System.currentTimeMillis())  + "|" + message);
            }
        });
        consumer.start();
        while (true) {

        }
    }

}
