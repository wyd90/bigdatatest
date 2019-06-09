package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkSqlWCDSL2xAPI {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksqlwcsql2xapi").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("/Users/wangyadi/sparkData/wc/input")

    import spark.implicits._
    //Dataset只有一列，只有一列的dataframe，认这列叫value
    //Dataset也是分布式数据集，是对RDD进一步封装，是更加智能的RDD
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = words.groupBy($"value" as "word")
      .agg(count($"*") as "cnts")
      .orderBy($"cnts" desc)

    result.show()

    spark.stop()
  }

}
