package com.wyd.stormtest.dataskew;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class SecondCountBolt implements IRichBolt {

    private OutputCollector collector;

    private Map<String, Integer> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        map = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = input.getInteger(1);
        if(!map.containsKey(word)){
            map.put(word, count);
        } else {
            Integer c = map.get(word) + count;
            map.put(word, c);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            System.out.println(entry.getKey() + ":::::::" + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
