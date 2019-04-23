package com.kafka;

import java.util.HashMap;
import java.util.Map;

public class Constants {

    public static final String TOPIC_NAME = "yz.biosecurityCenter.licensePlate";

//    public static final String TOPIC_NAME = "yz.breedingcenter.taskpool.event";

//    public static final String BROKERLIST = "kafka-node01.yingzi.com:9092,kafka-node02.yingzi.com:9092,kafka-node03.yingzi.com:9092";

    public static final String BROKERLIST = "kafka-dev.yingzi.local:9091,kafka-dev.yingzi.local:9092,kafka-dev.yingzi.local:9093";

//    public static final String BROKERLIST = "kafka-pre-prod01.yingzi.com:9094,kafka-pre-prod02.yingzi.com:9094,kafka-pre-prod03.yingzi.com:9094";


//    public static final String BROKERLIST = "172.19.100.128:9092";

    private static Map<String, String> userMap = null;

    private static Map<String, String> getUserMap() {
        if (userMap == null || userMap.isEmpty()) {
            userMap = new HashMap<>();
            userMap.put("admin", "vG5dfd2W");
            userMap.put("producer", "5ApsTaLR");
            userMap.put("consumer", "g7R2rSsW");
            userMap.put("biologicalCenterAdmin", "qhOM8X6d");
            userMap.put("nutritionCenterAdmin", "iZo4qAhH");
            userMap.put("breedingCenterAdmin", "NtkhN9Zv");
            userMap.put("geneticCenterAdmin", "YiMwRyAO");
            userMap.put("pigidentityCenterAdmin", "LEZP6RBm");
            userMap.put("iotCenterServerAdmin", "LONpxniG");
            userMap.put("iotCenterClientAdmin", "uI1BpIYw");
            userMap.put("taskCenterAdmin", "4N9W5Iz9");
            userMap.put("alarmCenterAdmin", "5TnZDIXs");

        }
        return userMap;
    }

    public static String getPassWorld(String userName){
        return getUserMap().get(userName);
    }
}
