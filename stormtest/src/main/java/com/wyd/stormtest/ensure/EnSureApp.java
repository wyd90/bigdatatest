package com.wyd.stormtest.ensure;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class EnSureApp {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ensureSpout", new WordCountWithEnSureSpout(), 2).setNumTasks(2);
        builder.setBolt("splitBolt", new SplitWithEnSureBolt(), 3).shuffleGrouping("ensureSpout").setNumTasks(3);

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, builder.createTopology());
        Thread.sleep(5000);
        cluster.shutdown();
    }
}
