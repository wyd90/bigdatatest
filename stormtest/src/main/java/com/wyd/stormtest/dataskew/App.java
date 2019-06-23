package com.wyd.stormtest.dataskew;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout", new WcSpout(), 2).setNumTasks(2);
        builder.setBolt("splitbolt", new SplitBolt(), 3).shuffleGrouping("wcspout").setNumTasks(3);
        builder.setBolt("countbolt", new CountBolt(), 5).shuffleGrouping("splitbolt").setNumTasks(5);
        builder.setBolt("secondcountbolt", new SecondCountBolt(), 2).fieldsGrouping("countbolt", new Fields("word")).setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(6);

        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wcskew", conf, builder.createTopology());

        Thread.sleep(20000);

        cluster.shutdown();
    }
}
