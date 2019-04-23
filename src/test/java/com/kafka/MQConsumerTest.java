package com.kafka;

import com.ygj.MessageConsumer;
import com.ygj.MessageHandler;
import org.junit.Test;

import java.util.*;
import java.util.regex.Pattern;

public class MQConsumerTest {


    @Test
    public void messageSSLConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-group");

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\Administrator\\Desktop\\certificates\\certificates-1\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.keystore.location", "C:\\Users\\Administrator\\Desktop\\certificates\\certificates-1\\kafka.keystore");
        props.put("ssl.keystore.password", "kafka1234567");
        props.put("ssl.key.password", "kafka1234567");

        MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(Constants.TOPIC_NAME), new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
                System.out.println(message);
            }
        });

        consumer.start();

        while (true) {


        }
    }

    @Test
    public void messageSASL_SSLConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"consumer\" password=\"g7R2rSsW\";");
        props.put("ssl.truststore.location", "D:\\Documents\\Downloads\\certificates_test\\certificates_test\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.keystore.location", "D:\\Documents\\Downloads\\certificates_test\\certificates_test\\kafka.keystore");
        props.put("ssl.keystore.password", "kafka1234567");
        props.put("ssl.key.password", "kafka1234567");
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

    @Test
    public void messageSASL_SSLConsumerTest2() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-group-2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iotCenterServerAdmin\" password=\"LONpxniG\";");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"consumer\" password=\"g7R2rSsW\";");
        props.put("ssl.truststore.location", "D:\\Documents\\Downloads\\certificates_20190201\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
//        props.put("ssl.keystore.location", "D:\\Documents\\Downloads\\certificates_20190201\\kafka.keystore");
//        props.put("ssl.keystore.password", "kafka1234567");
//        props.put("ssl.key.password", "kafka1234567");
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

    private void setConfig() {
        String resource = Thread.currentThread().getContextClassLoader().getResource("kafka_client_jaas.conf").getPath();
        System.setProperty("java.security.auth.login.config", resource);
    }

    @Test
    public void messageConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "333333333333333");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(Constants.TOPIC_NAME), new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
                System.out.println(message);

            }
        });
        consumer.start();
        while (true) {
        }
    }


    @Test
    public void messageSASLConsumerTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("client.id", "9912199");
        props.put("group.id", "test-0021232358");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"biosecurityCenterAdmin\" password=\"wBH14WeU\";");
//        List<String> list = new ArrayList<>();
//        list.add(Constants.TOPIC_NAME);
//        list.add("yz.taskcenter.realtimetask.event");
//        list.add("yz.taskcenter.canceltask.event");
//        list.add("yz.taskcenter.handletask.event");
//        list.add("yz.breedingcenter.pigstatus.location");

        List<String> list = new ArrayList<>();
        list.add("test01");
        list.add("test02");
        list.add("test03");
        list.add("test04");
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

    @Test
    public void messageSASLConsumerPatternTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("client.id", "9999");
        props.put("group.id", "test-0021259");
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"biosecurityCenterAdmin\" password=\"wBH14WeU\";");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iotCenterServerAdmin\" password=\"LONpxniG\";");

        final Map<String, Integer> countMap = new HashMap();
        countMap.put("key", 0);

        MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(Constants.TOPIC_NAME), new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
//                countMap.put("key", countMap.get("key") + 1);
//                System.out.println();
                System.out.println(message);
            }
        });

        consumer.start();

        while (true) {


        }
    }

    @Test
    public void messageSASLConsumerPatternTest2() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("client.id", "999549");
        props.put("group.id", "test-0021210");
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"consumer\" password=\"g7R2rSsW\";");
        String topicname = "yz.iotcenter.gatewayservice.up.*";
        Pattern pattern = Pattern.compile(topicname);
        MessageConsumer consumer = new MessageConsumer(props, pattern, new MessageHandler() {
            @Override
            public void handleMessage(String key, String message) {
                System.out.println(new Date(System.currentTimeMillis()) + ":key=" + key);
                System.out.println(message);
            }
        });

        consumer.start();

        while (true) {


        }
    }


}
