package com.wyd.javacode;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class KafkaSparkWithJavaDemo {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("kafkasparkDemo").setMaster("spark://node1:7077,node2:7077");


        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Integer i = 0;
                    for(Integer v : values){
                        i = i + v;
                    }
                    Integer newSum =   i + state.get();// add the new values with the previous running count to get the new count
                    return Optional.of(newSum);
                };

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "node2:9092,node3:9092,node4:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafkajavademo");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("kafka_java_wordcount");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD(rdd -> {
            if(!rdd.isEmpty()){
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                JavaRDD<String> lines = rdd.mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(tuple -> tuple._2);
                JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
                JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));

                JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((i1, i2) -> i1 + i2);
                JavaPairRDD<Integer, String> prepareToSort = reduced.mapToPair(row -> new Tuple2<>(row._2, row._1));

                JavaPairRDD<Integer, String> midSort = prepareToSort.sortByKey(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                JavaPairRDD<String, Integer> sorted = midSort.mapToPair(row -> new Tuple2<>(row._2, row._1));

                List<Tuple2<String, Integer>> collect = sorted.collect();
                for(Tuple2 t : collect){
                    System.out.println(t._1+":"+t._2);
                }

                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            }

        });


        jssc.start();
        jssc.awaitTermination();
    }
}
