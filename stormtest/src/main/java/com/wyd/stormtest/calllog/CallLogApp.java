package com.wyd.stormtest.calllog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class CallLogApp {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        //设置spout，设置两个龙头
        builder.setSpout("spout",new CallLogSpout()).setNumTasks(2);
        //设置Bolt spout产生的数据流以shuffle的形式发送给bolt
        builder.setBolt("creator-bolt",new CallLogCreatorBolt()).shuffleGrouping("spout");
        //按照call进行分组
        builder.setBolt("count-bolt",new CallLogCounterBolt()).fieldsGrouping("creator-bolt",new Fields("call"));

        Config conf = new Config();
        //conf.setDebug(true);
        //本地调试模式
        //LocalCluster cluster = new LocalCluster();
        //提交任务到集群
        //cluster.submitTopology("LogAnalys",conf,builder.createTopology());
        //Thread.sleep(10000);

        //手动停止集群
        //cluster.shutdown();

        StormSubmitter.submitTopology("mytop",conf,builder.createTopology());
    }
}
