package com.wyd.stormtest.kafkaintegration.write;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSourceSpout implements IRichSpout {

    private List<String> list = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private int index = 0;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if(index < 6 ){
            collector.emit(new Values(index+"",list.get(index)));
            index++;
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key","message"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
