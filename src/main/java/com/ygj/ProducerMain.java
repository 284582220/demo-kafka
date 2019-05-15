package com.ygj;

import java.util.Properties;

public class ProducerMain {

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    public static void main(String[] args) {

    }

    /**
     * 内网发送消息
     */
    private static void  messageProducerTestSASL() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 1024*16);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("request.timeout.ms", 50000);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
        MessageProducer producer = new MessageProducer(props);
        sendMessage(producer);
    }

    private static  void sendMessage(MessageProducer producer) throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            String result = "{\"result\":" + "123" + ", \"taskId\":" + i + " }" + System.currentTimeMillis();
            producer.sendMessage(Constants.TOPIC_NAME, null, result);
        }
    }


    /**
     * 外网发送消息
     */
    private static void messageProducerSASL_SSLTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("request.timeout.ms", 100000);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
        props.put("ssl.truststore.location","D:\\code\\git\\study\\kafka-core-demo\\cert\\pre-prod\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
        MessageProducer producer = new MessageProducer(props);
        sendMessage(producer);
    }

}
