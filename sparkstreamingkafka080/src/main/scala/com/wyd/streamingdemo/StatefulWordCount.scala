package com.wyd.streamingdemo

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StatefulWordCount {

  val updateFunc = (iter: Iterator[(String,Seq[Int],Option[Int])]) => {
    iter.map{
      case(x,y,z) => (x, y.sum + z.getOrElse(0))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Milliseconds(5000))

    ssc.checkpoint("/Users/wangyadi/checkpoint")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    result.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
