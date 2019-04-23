package com.kafka;

import com.ygj.MessageProducer;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicAclControllerProducerTest {


    private static Map<String, List<String>> userToTopic;

    private volatile int count = 0;


    @Before
    public void init() {
        userToTopic = new HashMap<>();
        List<String> bollogicalCenterTopicList = new ArrayList();
        bollogicalCenterTopicList.add("yz.biologicalcenter.taskpool.event");
        userToTopic.put("biologicalCenterAdmin", bollogicalCenterTopicList);

        List<String> nutritionCenterAdminTopicList = new ArrayList();
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.load.feed");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.feed");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.calcfeed.start");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.calcfeed.finish");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.taskpool.event");
        userToTopic.put("nutritionCenterAdmin", nutritionCenterAdminTopicList);


        List<String> breedingCenterAdminTopicList = new ArrayList();
        breedingCenterAdminTopicList.add("yz.breedingcenter.pigstatus.physical");
        breedingCenterAdminTopicList.add("yz.breedingcenter.pigstatus.location");
        breedingCenterAdminTopicList.add("yz.breedingcenter.pig.litter");
        breedingCenterAdminTopicList.add("yz.breedingcenter.taskpool.event");
        userToTopic.put("breedingCenterAdmin", breedingCenterAdminTopicList);



        List<String> geneticcenterAdminTopicList = new ArrayList();
        geneticcenterAdminTopicList.add("yz.geneticcenter.taskpool.event");
        userToTopic.put("geneticCenterAdmin", geneticcenterAdminTopicList);


        List<String> pigidentityCenterAdminTopicList = new ArrayList();
        pigidentityCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
        userToTopic.put("pigidentityCenterAdmin", pigidentityCenterAdminTopicList);


        List<String> iotCenterServerAdminTopicList = new ArrayList();
        iotCenterServerAdminTopicList.add("yz.iotcenter.topic");
        iotCenterServerAdminTopicList.add("yz.iotcenter.qrscan");
        userToTopic.put("iotCenterServerAdmin", iotCenterServerAdminTopicList);


        List<String> iotCenterClientAdminTopicList = new ArrayList();
        iotCenterClientAdminTopicList.add("yz.iotcenter.gatewayservice.upward");
        userToTopic.put("iotCenterClientAdmin", iotCenterClientAdminTopicList);


        List<String> taskCenterAdminTopicList = new ArrayList();
        taskCenterAdminTopicList.add("yz.taskcenter.realtimetask.event");

        userToTopic.put("taskCenterAdmin", taskCenterAdminTopicList);


        List<String> alarmCenterAdminTopicList = new ArrayList();
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.create");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.cancel");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.close");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.ignore");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.interrupt");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.level");
        alarmCenterAdminTopicList.add("yz.alarmcenter.alarmEvent.process");
        alarmCenterAdminTopicList.add("yz.alarmcenter.metric.record");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.confirm");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.help");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.ignore");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.receive");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.send");
        alarmCenterAdminTopicList.add("yz.alarmcenter.noticeEvent.unsend");
        userToTopic.put("alarmCenterAdmin", alarmCenterAdminTopicList);

    }

    @Test
    public void producerTest() throws ExecutionException, InterruptedException {
        String userName = "producer";
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
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName+ "\" password=\"" + Constants.getPassWorld(userName) + "\";");

        MessageProducer producer = new MessageProducer(props);

        List<String> topicList = initTopicList(userName);
        for(String topic : topicList){
            for (int i = 0; i < 1; i++) {
//                Thread.sleep(500L);
                System.out.println(userName + ":         " + topic);
                String result = "{\"producer\":" + userName + ", \"topic\":" + topic + ", \"taskId\":" + i + " }";
                count++;
                producer.sendMessage(topic, null, result);
            }
        }
        System.out.println("count:" + count);
    }

    private List<String> initTopicList(String userName) {
        List<String> topicList = new ArrayList<>();
        if(userName.equals("admin") || userName.equals("producer") || userName.equals("consumer")){
            for(String key : userToTopic.keySet()){
                List<String> userTopicList = userToTopic.get(key);
                topicList.addAll(userTopicList);
            }
        }else{
            topicList = userToTopic.get(userName);
        }
        return topicList;
    }


    private void sendMessage(MessageProducer producer) throws InterruptedException {
        for (int i = 10; i < 20; i++) {
            Thread.sleep(1000L);
            String result = "{\"result\":" + "123" + ", \"taskId\":" + i + " }" + System.currentTimeMillis();
            producer.sendMessage(Constants.TOPIC_NAME, null, result);
        }
    }


}
