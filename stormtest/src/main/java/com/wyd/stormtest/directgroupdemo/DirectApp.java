package com.wyd.stormtest.directgroupdemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class DirectApp {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WcSpout(),2).setNumTasks(2);
        builder.setBolt("splitbolt", new SplitBolt(), 5).directGrouping("spout").setNumTasks(5);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        //Thread.sleep(10000);

        //cluster.shutdown();
    }
}
