package com.wyd.streamingjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SteamingWordCountDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("file:///Users/wangyadi/checkpoint");
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Integer i = 0;
                    for(Integer v : values){
                        i = i + v;
                    }

                    Integer c = state.isPresent() ? state.get() : 0;
                    Integer newSum =   i + c;// add the new values with the previous running count to get the new count
                    return Optional.of(newSum);
                };

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> wordAndOne = words.mapToPair(x -> new Tuple2<>(x, 1));

        //JavaPairDStream<String, Integer> reduced = wordAndOne.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairDStream<String, Integer> wordcount = wordAndOne.updateStateByKey(updateFunction);

        wordcount.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
