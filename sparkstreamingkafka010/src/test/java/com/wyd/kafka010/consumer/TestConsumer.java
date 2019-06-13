package com.wyd.kafka010.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {

    @Test
    public void testAutomaticOffsetsCommiteConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers","node2:9092,node3:9092,node4:9092");
        //设置组id
        props.put("group.id","autocommitconsumer");
        //设置自动提交offsets
        props.put("enable.auto.commit","true");
        //设置自动提交时间间隔
        props.put("auto.commit.interval.ms","1000");

        //设置从什么位置开始读
        //当设置自动提交时，且当存在offset时，设置什么也不管用，都是从offset开始读
        //props.put("auto.offset.reset","latest"); //从记录的偏移量位置开始读
        props.put("auto.offset.reset","earliest"); //从topic的开始读

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //添加一个到多个topic
        consumer.subscribe(Arrays.asList("nmywordcount"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record: records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }

    }

    @Test
    public void testManualOffsets(){
        Properties props = new Properties();
        props.put("bootstrap.servers","node2:9092,node3:9092,node4:9092");
        //设置组id
        props.put("group.id", "manualcommitconsumer");
        //设置不进行自动提交offsets
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("nmywordcount"));
        ArrayList<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        final int minBatchSize = 1;
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records){
                buffer.add(record);
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            if(buffer.size() >= minBatchSize){
                consumer.commitSync();
                buffer.clear();
            }
        }


    }
}
