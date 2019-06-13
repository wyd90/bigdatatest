package com.wyd.mongospark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import com.mongodb.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingWCWriteMongo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWCWriteMongo").setMaster("local[*]")
      .set("spark.mongodb.output.uri","mongodb://admin:az63091919@192.168.56.101:27017,192.168.56.102:27017,192.168.56.103:27017/mobike.wordcount?authSource=admin")

    val ssc = new StreamingContext(conf, Duration(5000))

    val groupId = "mwc1"

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "node2:9092,node3:9092,node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("mongowc")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(kafkaRDD => {
      if(!kafkaRDD.isEmpty()){
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        val lines = kafkaRDD.map(x => x.value())

        val reduced = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

        val spark = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
        import spark.implicits._

        val result: DataFrame = reduced.toDF("word","count")

        result.write.mode("append").mongo()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _
  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}