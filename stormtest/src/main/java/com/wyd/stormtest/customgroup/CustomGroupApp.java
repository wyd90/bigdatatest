package com.wyd.stormtest.customgroup;

import com.wyd.stormtest.dataskew.SplitBolt;
import com.wyd.stormtest.dataskew.WcSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class CustomGroupApp {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout", new WcSpout(), 2).setNumTasks(2);
        builder.setBolt("splitbolt", new SplitBolt(), 4).customGrouping("wcspout", new MyGrouping()).setNumTasks(4);

        Config conf = new Config();
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("customgroup", conf, builder.createTopology());

    }
}
