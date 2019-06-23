package com.wyd.stormtest.kafkaintegration.read;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;

public class ReadingFromKafkaApp {

    private static final String BOOTSTRAP_SERVERS = "node2:9092,node3:9092,node4:9092";

    private static final String TOPIC_NAME = "storm-wc";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(getKafkaSpoutConfig(BOOTSTRAP_SERVERS,TOPIC_NAME)) , 3).setNumTasks(3);
        builder.setBolt("logConsole-bolt", new LogConsoleBolt(), 1).shuffleGrouping("kafka-spout").setNumTasks(1);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkatest", conf, builder.createTopology());

    }

    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers, String topic){
        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormconsumer")
                //设定重试策略
                .setRetry(getRetryService())
                //定时提交偏移量的时间间隔，默认是15s
                .setOffsetCommitPeriodMs(10000)
                .build();
    }

    //定义重试策略
    private static KafkaSpoutRetryService getRetryService(){
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                10, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }



}
