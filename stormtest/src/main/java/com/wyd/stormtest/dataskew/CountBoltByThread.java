package com.wyd.stormtest.dataskew;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CountBoltByThread implements IRichBolt {

    private OutputCollector collector;

    private Map<String, Integer> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        map = new HashMap<String, Integer>();
        //把map变成线程安全的集合
        map = Collections.synchronizedMap(this.map);
        Thread thread = new Thread() {
            @Override
            public void run() {
                emitData();
            }
        };
        //设置线程为守护线程
        thread.setDaemon(true);
        thread.start();
    }

    private void emitData(){
        while (true){
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //清分map，对map加锁，在清分时不允许再往map里添加
            synchronized (map){
                for(Map.Entry<String, Integer> entry : map.entrySet()){
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                }
                map.clear();
            }
        }

    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        if(!map.containsKey(word)){
            map.put(word, 1);
        } else {
            int c = map.get(word) + 1;
            map.put(word, c);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
