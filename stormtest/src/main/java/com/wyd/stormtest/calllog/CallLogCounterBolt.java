package com.wyd.stormtest.calllog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CallLogCounterBolt implements IRichBolt {

    private Map<String,Integer> counterMap;
    private  OutputCollector collector;

    /**
     *
     * @param stormConf
     * @param context
     * @param collector
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counterMap = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String call = input.getString(0);
        Integer duration = input.getInteger(1);

        if(!counterMap.containsKey(call)){
            counterMap.put(call,1);
        } else {
            Integer c = counterMap.get(call) + 1;
            counterMap.put(call,c);
        }
        //确认消息已经被消费
        collector.ack(input);
    }

    public void cleanup() {

            for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                System.out.println(entry.getKey() + " ------------ " + entry.getValue());
            }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
