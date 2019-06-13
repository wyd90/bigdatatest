package com.wyd.streamingkafka

import java.lang

import com.wyd.redisutil.JedisClusterConnectPool
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.JedisCluster

object Kafka010DirectWordCount {

  def main(args: Array[String]): Unit = {
    val group = "ng0"
    val topic = "nmywordcount"

    val conf = new SparkConf().setAppName("Kafka010DirectWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Duration(5000))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "node2:9092,node3:9092,node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", //lastest
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array(topic)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略（如果kafka和spark程序部署在一起，会有最优位置感知）
      LocationStrategies.PreferConsistent,
      //订阅的策略（可以指定用正则的方式读取topic，比如my-orders-*）
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(kafkaRDD =>{
      if(!kafkaRDD.isEmpty()){
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines: RDD[String] = kafkaRDD.map(x => x.value())
        val reduce: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

        reduce.foreachPartition(it => {
          val conn: JedisCluster = JedisClusterConnectPool.getConnection()
          it.foreach(x => {
            conn.incrBy(x._1, x._2.toLong)
          })
        })

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
