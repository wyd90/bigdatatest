package com.wyd.stormtest.dataskew;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WcSpout implements IRichSpout {

    private SpoutOutputCollector collector;

    private List<String> book;

    private Random random = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        book = new ArrayList<String>();
        book.add("hello world tom and jack");
        book.add("hello tom world hello");
        book.add("world tom marry and jack hello");
        book.add("hello hello hank tom world hello");

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
        String line = book.get(random.nextInt(4));
        collector.emit(new Values(line));
        try {
            Thread.sleep(2000);
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
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
