package com.wyd.ordersatistic

import java.lang

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * A 202.106.196.115 手机 iPhone8 8000
  * B 202.106.0.20 服装 布莱奥尼西服 199
  * C 202.102.152.3 家具 婴儿床 2000
  * D 202.96.96.68 家电 电饭锅 1000
  * F 202.98.0.68 化妆品 迪奥香水 200
  * H 202.96.75.68 食品 奶粉 600
  * J 202.97.229.133 图书 Hadoop编程指南 90
  * A 202.106.196.115 手机 手机壳 200
  * B 202.106.0.20 手机 iPhone8 8000
  * C 202.102.152.3 家具 婴儿车 2000
  * D 202.96.96.68 家具 婴儿车 1000
  * F 202.98.0.68 化妆品 迪奥香水 200
  * H 202.96.75.68 食品 婴儿床 600
  * J 202.97.229.133 图书 spark实战 80
  */
object OrderCalculate {

  var stopFlag:Boolean = false
  val shutdownMarker = "hdfs://hdp12/tmp/shutdownmarker"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ordercalculate")

    val ssc = new StreamingContext(conf, Duration(1000))

    val lines: RDD[String] = ssc.sparkContext.textFile("sparkData/ipLocation/rules")

    val rulesRDD: RDD[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      (fields(2).toLong, fields(3).toLong, fields(6))
    })

    val rules: Array[(Long, Long, String)] = rulesRDD.collect()
    val broadcastRules: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(rules)

    val groupId = "newOrderCalculate"

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "node2:9092,node3:9092,node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array("newOrderList")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(kafkaRDD => {
      if(!kafkaRDD.isEmpty()){
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        val lines: RDD[String] = kafkaRDD.map(x => x.value())
        val orders: RDD[Array[String]] = lines.map(line => {
          val arr: Array[String] = line.split(" ")
          arr
        })

        CalculateUtils.calculateIncome(orders)
        CalculateUtils.calculateByItem(orders)
        CalculateUtils.calculateIncomeByZone(orders, broadcastRules)

        //更新kafka自己管理的偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    ssc.start()

    val checkIntervalMillis = 10000
    var isStopped = false

    while (! isStopped) {
      println("calling awaitTerminationOrTimeout")
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running. Timeout...")
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        ssc.stop(true, true)
        println("ssc is stopped!!!!!!!")
      }
    }

  }

  def checkShutdownMarker = {
    if (!stopFlag) {
      val fs = FileSystem.get(new Configuration())
      stopFlag = fs.exists(new Path(shutdownMarker))
    }

  }

}
