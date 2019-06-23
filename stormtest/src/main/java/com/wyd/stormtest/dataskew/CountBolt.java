package com.wyd.stormtest.dataskew;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {

    private OutputCollector collector;

    //最后一次清分时间
    private long lastEmitTime = 0L;
    //清分间隔时间5000ms
    private long duration = 5000L;

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
        long now = System.currentTimeMillis();
        if((now - lastEmitTime) >= duration){
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            map.clear();
            lastEmitTime = now;
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
