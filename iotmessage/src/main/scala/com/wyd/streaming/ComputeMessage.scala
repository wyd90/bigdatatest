package com.wyd.streaming

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.wyd.redisutil.JedisClusterConnectPool
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.JedisCluster

import scala.util.parsing.json.JSON

object ComputeMessage {

  def isGoodJson(json: String): Boolean = {
    if(null == json) {
      return false;
    }

    JSON.parseFull(json) match  {
      case Some(_: Map[String, Any]) => true
      case None => false
      case _ => false
    }
  }

  def main(args: Array[String]): Unit = {



    val conf: SparkConf = new SparkConf().setAppName("computeMsg").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc: StreamingContext = new StreamingContext(conf, Duration(5000))


    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "node2:9092,node3:9092,node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stramingreceiver",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: lang.Boolean),
      "auto.commit.interval.ms" -> (60 * 1000: Integer)
    )

    val topics = Array("iotmsg")

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val windowStream: DStream[ConsumerRecord[String, String]] = kafkaStream.window(Duration(60000), Duration(60000))

    val temp1: DStream[(String, String, String, String, String)] = windowStream.map(c => c.value()).map(line => {
      if (isGoodJson(line)) {
        val jsonMap: Map[String, String] = JSON.parseFull(line).get.asInstanceOf[Map[String, String]]
        (jsonMap("v"), jsonMap("orgId"), jsonMap("devId"), jsonMap("equipmentId"), jsonMap("t"))
      } else {
        ("", "", "", "", "")
      }
    })
    val temp2: DStream[(String, String, String, String, String)] = temp1.filter(x => {
      if (x._2 == "") {
        false
      } else {
        true
      }
    })

    val temp3: DStream[(Double, String, String, String, String, String)] = temp2.mapPartitions(it => {
      val conn: JedisCluster = JedisClusterConnectPool.getConnection()
      val tuples = new scala.collection.mutable.ArrayBuffer[(Double, String, String, String, String, String)]()
      val sdf = new SimpleDateFormat("yyyyMMddHHmm")
      val time = new Date().getTime
      val timeStr: String = sdf.format(time)
      it.foreach(x => {
        val opType: String = conn.get(x._4)
        val a = (x._1.toDouble, x._2, x._3, x._4, timeStr, opType)
        tuples += a
      })
      tuples.toIterator
    })

    val avgOp: DStream[(Double, String, String, String, String, String)] = temp3.filter(_._6 == "AVG")

    val avgOp1: DStream[((String, String, String), (Double, Int,String))] = avgOp.map(x => {
      ((x._2, x._3, x._4), (x._1, 1, x._5))
    })

    val avgOp2: DStream[((String, String, String), (Double, Int, String))] = avgOp1.reduceByKey((a, b) => (a._1 + b._1 , a._2 + b._2, a._3))

    val avgOp3: DStream[((String, String, String), (Double, String))] = avgOp2.mapValues(x => {
      (x._1 / x._2, x._3)
    })

//    avgOp3.foreachRDD(rdd => {
//      rdd.foreach(x => {
//        println(x._1 + "," + x._2)
//      })
//    })

    ssc.sparkContext.hadoopConfiguration.set(HConstants.ZOOKEEPER_QUORUM,"node3:2181,node4:2181,node5:2181")
    ssc.sparkContext.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "iotMsg")

    val job: Job = Job.getInstance(ssc.sparkContext.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val avgOp4: DStream[(ImmutableBytesWritable, Put)] = avgOp3.map(x => {
      val put = new Put(Bytes.toBytes(x._1._1 + x._1._2 + x._1._3 + x._2._2))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("v"), Bytes.toBytes(x._2._1))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("t"), Bytes.toBytes(x._2._2))
      (new ImmutableBytesWritable, put)
    })

    avgOp4.foreachRDD(rdd => {
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
