package com.wyd.stormtest.ensure;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Random;

public class SplitWithEnSureBolt implements IRichBolt {

    private OutputCollector collector;

    private Random random = new Random();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] split = line.split(" ");
        if(random.nextBoolean()){
            System.out.println(this + " : execute() : " + line + " : success");
            collector.ack(input);
        } else {
            System.out.println(this + " : execute() : " + line + " : failure");
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
