package com.wyd.kafka010.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

public class Testproducer {

    @Test
    public void testProducer() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers","node2:9092,node3:9092,node4:9092");

        /**
         * Set acknowledgements for producer requests
         * acks=0: 意思server不会返回任何确认消息，不保证server是否收到，因为没有返回retires重试机制不会起效
         * acks=1: 意思是partition leader已确认写record到日志中，但是不保证record是否被正确复制(建议设置1)
         * acks=all: 意思是leader将等待所有同步复制broker的ack信息后返回
         */
        props.put("acks", "1");
        /**
         * 1.If the request fails, the producer can automatically retry,
         * 2.请设置大于0，这个重试机制与我们手动发起resend没有什么不同。
         */
        props.put("retries", 1);
        /**
         * 1.Specify buffer size in config
         * 2. 10.0后product完全支持批量发送给broker，不乱你指定不同parititon，product都是批量自动发送指定parition上。
         * 3. 当batch.size达到最大值就会触发dosend机制。
         */
        props.put("batch.size", 16384);
        /**
         * Reduce the no of requests less than 0;意思在指定batch.size数量没有达到情况下，在5s内也回推送数据
         */
        props.put("linger.ms", 1);
        /**
         * 1. The buffer.memory controls the total amount of memory available to the producer for buffering.
         * 2. 生产者总内存被应用缓存，压缩，及其它运算。
         *
         */
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>("nmywordcount","1","hello word tom and kitty"));
        Thread.sleep(5000);
        producer.send(new ProducerRecord<String, String>("nmywordcount","2","tom love kitty"));
        Thread.sleep(5000);
        producer.send(new ProducerRecord<String, String>("nmywordcount","3","hello hello hello tom"));
        producer.close();


    }

}
