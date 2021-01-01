package com.wyd.structurestreaming

import org.apache.spark.sql.SparkSession

object KafkaStreamingSourceDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kafkastreamsourcedemo")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import spark.sql

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
      .option("subscribe", "structurewc")
      .option("startingOffsets","earliest")
      .load()

    df
      .selectExpr("CAST(value as String)")
      .as[(String)]
      .flatMap(_.split(" "))
      .createOrReplaceTempView("t_wc")

    sql("select value,count(1) cnts from t_wc group by value")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start().awaitTermination()

    spark.stop()



  }

}
