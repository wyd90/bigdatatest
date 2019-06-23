package com.wyd.stormtest.wordcount;

import com.wyd.stormtest.util.Util;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * nc -lk 8888
 */
public class StormWordCount {
    public static class WcSpout implements IRichSpout{

        private SpoutOutputCollector collector;

        private Random random = new Random();

        private List<String> book = new ArrayList<String>();

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

            Util.info(this, "wcspout.open()");

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
            collector.emit(new Values(line));
            //Util.info(this, "wcspout.nextTuple()");
            try {
                Thread.sleep(1000);
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
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WcMapBolt implements IRichBolt{

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            Util.info(this, "wcmapbolt.prepare()");
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            //Util.info(this, "wcmapbolt.execute()");
            String line = input.getString(0);
            String[] words = line.split(" ");
            for(String word : words){
                collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","cnts"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WcReduceBolt implements IRichBolt{

        private OutputCollector collector;

        private Map<String, Integer> wordcount;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            Util.info(this, "wcreducebolt.prepare()");
            this.collector = collector;
            wordcount = new HashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple input) {
            //Util.info(this, "wcreducebolt.execute()");
            String word = input.getString(0);
            if(!wordcount.containsKey(word)){
                wordcount.put(word, 1);
            } else {
                int sum = wordcount.get(word) + 1;
                wordcount.put(word, sum);
            }
        }

        @Override
        public void cleanup() {
            for(Map.Entry<String, Integer> entry : wordcount.entrySet()){
                System.out.println(entry.getKey() + "----------" + entry.getValue());
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

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutwc", new WcSpout(),2).setNumTasks(2);
        builder.setBolt("map", new WcMapBolt(),3).shuffleGrouping("spoutwc").setNumTasks(3);
        builder.setBolt("reduce", new WcReduceBolt(),4).fieldsGrouping("map", new Fields("word")).setNumTasks(4);

        Config conf = new Config();
        conf.setNumWorkers(4);
        //conf.setDebug(true);

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("mywordcount", conf, builder.createTopology());
//
//        Thread.sleep(10000);
//
//        cluster.shutdown();
        StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());
    }
}
