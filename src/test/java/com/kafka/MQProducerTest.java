package com.kafka;

import com.ygj.MessageProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class MQProducerTest {


    @Test
    public void producerTest() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
//        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 1024*16);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        MessageProducer producer = new MessageProducer(props);
        sendMessage(producer);
    }


    @Test
    public void messageProducerTestSASL() throws Exception {
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
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iotCenterClientAdmin\" password=\"uI1BpIYw\";");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"biosecurityCenterAdmin\" password=\"wBH14WeU\";");
        MessageProducer producer = new MessageProducer(props);


        sendMessage(producer);
    }

    private void sendMessage(MessageProducer producer) throws InterruptedException {
        for (int i = 0; i < 5; i++) {
//            Thread.sleep(10L);
            String result = "{\"result\":" + "123" + ", \"taskId\":" + i + " }" + System.currentTimeMillis();
            producer.sendMessage(Constants.TOPIC_NAME, null, result);
        }
    }

    private void setConfig() {
        String resource = Thread.currentThread().getContextClassLoader().getResource("kafka_client_jaas.conf").getPath();
        System.setProperty("java.security.auth.login.config", resource);
    }

    @Test
    public void messageProducerSASL_SSLTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("acks", "all");
        props.put("retries", 10);
        props.put("request.timeout.ms", 100000);

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"producer\" password=\"5ApsTaLR\";");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iotCenterClientAdmin\" password=\"uI1BpIYw\";");
        props.put("ssl.truststore.location","D:\\code\\git\\study\\kafka-core-demo\\cert\\pre-prod\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
//        props.put("ssl.keystore.location", "D:\\code\\git\\study\\kafka-core-demo\\cert\\pre-prod\\kafka.keystore");
//        props.put("ssl.keystore.password", "kafka1234567");
//        props.put("ssl.key.password", "kafka1234567");
        MessageProducer producer = new MessageProducer(props);
        sendMessage(producer);
    }

    @Test
    public void messageProducerSSLTest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("acks", "all");
        props.put("retries", 10);
        props.put("request.timeout.ms", 10000);
        // 服务器#ssl.client.auth=required 注释掉，是否开启客户端认证，开启后，客户端需要输入认证信息，比如SASL或者签名认证

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location","C:\\Users\\Administrator\\Desktop\\certificates\\certificates-1\\kafka.truststore");
        props.put("ssl.truststore.password", "kafka1234567");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.keystore.location", "C:\\Users\\Administrator\\Desktop\\certificates\\certificates-1\\kafka.keystore");
        props.put("ssl.keystore.password", "kafka1234567");
        props.put("ssl.key.password", "kafka1234567");
        MessageProducer producer = new MessageProducer(props);
        sendMessage(producer);
    }

}
