package com.wyd.structurestreaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("wordcount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    val wc = lines.as[String].flatMap(_.split(" "))
      .groupBy("value")
      .count()


    val result: StreamingQuery = wc.writeStream
        .outputMode("complete")   //complete  append  update
        .format("console")
        .start()

    result.awaitTermination()
    spark.stop()

  }

}
