package com.wyd.kafka080spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object Kafka080HighAPIWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafka080HighAPIWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val zkQuorm = "node3:2181,node4:2181,node5:2181"

    val groupId = "ng1"
    //topic和对应的线程
    val topic = Map[String,Int]("nmytopic" -> 1)

    val kafkaData: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorm, groupId, topic)

    //取出消息的值
    val lines: DStream[String] = kafkaData.map(_._2)

    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
