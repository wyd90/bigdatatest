package com.wyd.stormtest.kafkaintegration.read;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 这里从value字段中获取kafka输出的值数据。
 *
 * 在开发中，我们可以通过继承RecordTranslator接口定义了Kafka中Record与输出流之间的映射关系，可以在构建KafkaSpoutConfig的时候通过构造器或者setRecordTranslator()方法传入，并最后传递给具体的KafkaSpout。
 *
 * 默认情况下使用内置的DefaultRecordTranslator，其源码如下，FIELDS中 定义了tuple中所有可用的字段：主题，分区，偏移量，消息键，值。
 *
 * public class DefaultRecordTranslator<K, V> implements RecordTranslator<K, V> {
 *     private static final long serialVersionUID = -5782462870112305750L;
 *     public static final Fields FIELDS = new Fields("topic", "partition", "offset", "key", "value");
 *     @Override
 *     public List<Object> apply(ConsumerRecord<K, V> record) {
 *         return new Values(record.topic(),
 *                 record.partition(),
 *                 record.offset(),
 *                 record.key(),
 *                 record.value());
 *     }
 *
 *     @Override
 *     public Fields getFieldsFor(String stream) {
 *         return FIELDS;
 *     }
 *
 *     @Override
 *     public List<String> streams() {
 *         return DEFAULT_STREAM;
 *     }
 * }
 */
public class LogConsoleBolt implements IRichBolt {

    private  OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String value = input.getStringByField("value");
        System.out.println("received from kafka: "+ value);
        collector.ack(input);
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
