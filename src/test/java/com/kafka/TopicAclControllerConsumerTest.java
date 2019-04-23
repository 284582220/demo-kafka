package com.kafka;

import com.ygj.MessageConsumer;
import com.ygj.MessageHandler;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TopicAclControllerConsumerTest {

    private static Map<String, List<String>> userToTopic;         // 用户对应的消费topic列表

    private volatile int count = 0;

    @Before
    public void init() {
        userToTopic = new HashMap<>();
        List<String> bollogicalCenterTopicList = new ArrayList();
        bollogicalCenterTopicList.add("yz.biologicalcenter.taskpool.event");
        bollogicalCenterTopicList.add("yz.breedingcenter.pig.litter");
        bollogicalCenterTopicList.add("yz.breedingcenter.pigstatus.physical");
        bollogicalCenterTopicList.add("yz.breedingcenter.pigstatus.location");
        bollogicalCenterTopicList.add("yz.pigidentitycenter.identity.event");
        userToTopic.put("biologicalCenterAdmin", bollogicalCenterTopicList);

        List<String> nutritionCenterAdminTopicList = new ArrayList();
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.load.feed");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.feed");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.calcfeed.start");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.calcfeed.finish");
        nutritionCenterAdminTopicList.add("yz.nutritioncenter.taskpool.event");
        nutritionCenterAdminTopicList.add("yz.breedingcenter.pigstatus.physical");
        nutritionCenterAdminTopicList.add("yz.breedingcenter.pigstatus.location");
        nutritionCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
        userToTopic.put("nutritionCenterAdmin", nutritionCenterAdminTopicList);


        List<String> breedingCenterAdminTopicList = new ArrayList();
        breedingCenterAdminTopicList.add("yz.breedingcenter.pigstatus.physical");
        breedingCenterAdminTopicList.add("yz.breedingcenter.pigstatus.location");
        breedingCenterAdminTopicList.add("yz.breedingcenter.pig.litter");
        breedingCenterAdminTopicList.add("yz.breedingcenter.taskpool.event");
        breedingCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
        userToTopic.put("breedingCenterAdmin", breedingCenterAdminTopicList);



        List<String> geneticcenterAdminTopicList = new ArrayList();
        geneticcenterAdminTopicList.add("yz.geneticcenter.taskpool.event");
        userToTopic.put("geneticCenterAdmin", geneticcenterAdminTopicList);


        List<String> pigidentityCenterAdminTopicList = new ArrayList();
        pigidentityCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
        pigidentityCenterAdminTopicList.add("yz.breedingcenter.pigstatus.physical");
        pigidentityCenterAdminTopicList.add("yz.breedingcenter.pigstatus.location");
        pigidentityCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
        userToTopic.put("pigidentityCenterAdmin", pigidentityCenterAdminTopicList);


        List<String> iotCenterServerAdminTopicList = new ArrayList();
        iotCenterServerAdminTopicList.add("yz.iotcenter.topic");
        iotCenterServerAdminTopicList.add("yz.iotcenter.qrscan");

        iotCenterServerAdminTopicList.add("yz.nutritioncenter.feed");
        iotCenterServerAdminTopicList.add("yz.nutritioncenter.load.feed");
        iotCenterServerAdminTopicList.add("yz.iotcenter.gatewayservice.upward");
        userToTopic.put("iotCenterServerAdmin", iotCenterServerAdminTopicList);


        List<String> iotCenterClientAdminTopicList = new ArrayList();
        iotCenterClientAdminTopicList.add("yz.iotcenter.gatewayservice.upward");
        userToTopic.put("iotCenterClientAdmin", iotCenterClientAdminTopicList);


        List<String> taskCenterAdminTopicList = new ArrayList();
        taskCenterAdminTopicList.add("yz.taskcenter.realtimetask.event");
        taskCenterAdminTopicList.add("yz.biologicalcenter.taskpool.event");
        taskCenterAdminTopicList.add("yz.nutritioncenter.taskpool.event");
        taskCenterAdminTopicList.add("yz.breedingcenter.taskpool.event");
        taskCenterAdminTopicList.add("yz.geneticcenter.taskpool.event");
        taskCenterAdminTopicList.add("yz.breedingcenter.pigstatus.physical");
        taskCenterAdminTopicList.add("yz.breedingcenter.pigstatus.location");
        taskCenterAdminTopicList.add("yz.pigidentitycenter.identity.event");
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
    public void messageSASLConsumerTest() throws Exception {
        String userName = "biologicalCenterAdmin";
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKERLIST);
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName+ "\" password=\"" + Constants.getPassWorld(userName) + "\";");
        List<String> topicList = getConsumerTopic(userName);
        for(String topic : topicList){
            MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(topic),  new MessageHandler() {
                @Override
                public void handleMessage(String key, String message) {
                    System.out.println(message);
                    count++;
                    System.out.println("count:" + count);
                }
            });
            consumer.start();
        }
        while(true){
        }
    }

    private List<String> getConsumerTopic(String userName) {
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

}
