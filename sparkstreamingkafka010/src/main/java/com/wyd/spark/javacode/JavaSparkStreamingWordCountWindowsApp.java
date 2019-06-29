package com.wyd.spark.javacode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/4/3.
 */
public class JavaSparkStreamingWordCountWindowsApp {
    static JavaReceiverInputDStream sock;
    public static void main(String[] args) throws Exception {
        Function0<JavaStreamingContext> contextFactory = new Function0<JavaStreamingContext>() {
            //首次创建context时调用该方法。
            public JavaStreamingContext call() {
                SparkConf conf = new SparkConf();
                conf.setMaster("local[4]");
                conf.setAppName("wc");
                JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(2000));
                JavaDStream<String> lines = jssc.socketTextStream("localhost",9999);

                /*******  变换代码放到此处 ***********/
                //JavaDStream<Long> dsCount = lines.countByWindow(new Duration(24 * 60 * 60 * 1000),new Duration(2000));
                JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
                JavaPairDStream<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
                JavaPairDStream<String, Integer> reduced = wordAndOne.reduceByKeyAndWindow((i1, i2) -> i1 + i2, new Duration(24 * 60 * 60 * 1000), new Duration(4000));
                //dsCount.print();

                //设置检察点目录
                jssc.checkpoint("file:///Users/wangyadi/checkpoint");
                reduced.checkpoint(new Duration(8000));
                reduced.print();
                return jssc;
            }
        };
        //失败重建时会经过检查点。
        JavaStreamingContext context = JavaStreamingContext.getOrCreate("file:///Users/wangyadi/checkpoint", contextFactory);

        context.start();
        context.awaitTermination();
    }
}

