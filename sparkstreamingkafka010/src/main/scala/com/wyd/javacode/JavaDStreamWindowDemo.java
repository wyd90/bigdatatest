package com.wyd.javacode;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaDStreamWindowDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("windowDemo").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> reduced = wordAndOne.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(12), Durations.seconds(4));

        reduced.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
