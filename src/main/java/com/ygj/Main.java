package com.ygj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static List<String> topicNameList = Arrays.asList("yz.biologicalcenter.taskpool.event", "yz.breedingcenter.check.affection", "yz.breedingcenter.pig.litter", "yz.breedingcenter.pigstatus.location",
            "yz.breedingcenter.pigstatus.physical", "yz.breedingcenter.taskpool.event", "yz.geneticcenter.taskpool.event", "yz.metricRankingCenter.metric.record", "yz.nutritioncenter.calcfeed.finish", "yz.nutritioncenter.calcfeed.start",
    "yz.nutritioncenter.feed", "yz.nutritioncenter.load.feed", "yz.nutritioncenter.plan.feed", "yz.nutritioncenter.plan.load.feed", "yz.nutritioncenter.taskpool.event", "yz.pigidentitycenter.identity.event", "yz.taskcenter.canceltask.event",
            "yz.taskcenter.handletask.event", "yz.taskcenter.pushmating.event", "yz.taskcenter.realtimetask.event");

    public static void main(String[] args) throws Exception {
        if(args.length != 1){
            throw new Exception("please enter bootstrap.servers");
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", "test-ygj-01");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"consumer\" password=\"g7R2rSsW\";");

        for(String topicName : topicNameList){
            props.put("client.id", topicName + "-ygj-01");
            MessageConsumer consumer = new MessageConsumer(props, Arrays.asList(topicName), new MessageHandler() {
                @Override
                public void handleMessage(String key, String message) {
                    log.info(topicName + new Date(System.currentTimeMillis()) + ":key=" + key + ";message=" + message);
                }
            });
            consumer.start();
        }

        while (true) {
        }
    }
}
