package com.wyd.streamingkafka

import java.lang

import com.wyd.redisutil.JedisClusterConnectPool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.JedisCluster
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * sparkstreaming读kafka数据计算wordcount偏移量存到mysql里
  *
  * mysql建表语句
  * CREATE TABLE `offset` (
  * `groupId` varchar(255) DEFAULT NULL,
  * `topic` varchar(255) DEFAULT NULL,
  * `partition` int(11) DEFAULT NULL,
  * `untilOffset` bigint(20) DEFAULT NULL
  * ) ENGINE=InnoDB DEFAULT CHARSET=utf8
  *
  */
object Kafka010DirectWordCount {

  def main(args: Array[String]): Unit = {
    val group = "ng1"
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

    DBs.setup()
    val fromdbOffsets = DB.readOnly(
      implicit session => {
        SQL(s"select * from offset where groupId ='${group}'")
          .map(rs => {
            (new TopicPartition(rs.string("topic"), rs.int("partition")), rs.long("untilOffset"))
          }).list().apply()
      }
    ).toMap

    val stream = if(fromdbOffsets.size == 0){
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String,String](fromdbOffsets.keys, kafkaParams, fromdbOffsets)
      )
    }



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

        DB.localTx(
          implicit session => {
            for(or <- offsetRanges){
              SQL("replace into `offset` (groupId,topic,`partition`,untilOffset) values(?,?,?,?)")
                .bind(group, or.topic, or.partition, or.untilOffset).update().apply()
            }
          }
        )

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
