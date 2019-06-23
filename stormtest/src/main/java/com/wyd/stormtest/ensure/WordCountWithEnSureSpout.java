package com.wyd.stormtest.ensure;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

public class WordCountWithEnSureSpout implements IRichSpout {

    private SpoutOutputCollector collector;

    //消息集合，存放所有消息
    private Map<Long, String> messages = new HashMap<Long, String>();
    //失败消息列表，msgId，次数
    private Map<Long, Integer> failMessages = new HashMap<Long, Integer>();

    private List<String> book = new ArrayList<String>();

    private Random random = new Random();
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        book.add("hello world tom and hank");
        book.add("ha ha marry jack");
        book.add("hello hank tom and lee");
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
        String line = book.get(random.nextInt(3));
        long ts = System.currentTimeMillis();
        collector.emit(new Values(line),ts);
        messages.put(ts, line);
        System.out.println(this + " : " + "nextTuple() : " + line+" : " + ts);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        System.out.println(this + " : " + "ack() : "+msgId);
        Long ts = (Long)msgId;
        if(failMessages.containsKey(ts)){
            failMessages.remove(ts);
        }
        if(messages.containsKey(ts)){
            messages.remove(ts);
        }
    }

    @Override
    public void fail(Object msgId) {
        System.out.println(this + " : " + "fail() : "+msgId);
        Long ts = (Long)msgId;
        Integer count = failMessages.get(ts);
        if (count != null){
            if(count >= 3){
                failMessages.remove(ts);
            } else {
                count ++;
                reemit(ts, count);
            }
        } else {
            reemit(ts, 1);
        }
    }

    private void reemit(Long ts, Integer count){
        failMessages.put(ts, count);
        String line = messages.get(ts);
        collector.emit(new Values(line), ts);
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
