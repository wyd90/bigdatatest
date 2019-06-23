package com.wyd.stormtest.kafkaintegration.write;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

public class WriteToKafkaApp {

    private static final String BOOTSTRAP_SERVERS = "node2:9092,node3:9092,node4:9092";

    private static final String TOPIC_NAME = "storm-produce";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        /*
         * acks 参数指定了必须要有多少个分区副本收到消息，生产者才会认为消息写入是成功的。
         * acks=0 : 生产者在成功写入消息之前不会等待任何来自服务器的响应。
         * acks=1 : 只要集群的首领节点收到消息，生产者就会收到一个来自服务器成功响应。
         * acks=all : 只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
         */
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(TOPIC_NAME))
                                                                                                //可以写，默认就是key和message，如果spout输出的是别的名称就要加上
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key","message"));

        builder.setSpout("sourceSpout", new DataSourceSpout(), 1).setNumTasks(1);
        builder.setBolt("kafkaBolt", kafkaBolt, 3).shuffleGrouping("sourceSpout").setNumTasks(3);

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("writeToKafka", conf, builder.createTopology());

    }
}
