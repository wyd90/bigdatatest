package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlWCSQL2xAPI {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksqlwcsql2xapi").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("/Users/wangyadi/sparkData/wc/input")

    import spark.implicits._
    //Dataset只有一列，只有一列的dataframe，认这列叫value
    //Dataset也是分布式数据集，是对RDD进一步封装，是更加智能的RDD
    val words: Dataset[String] = lines.flatMap(_.split(" "))


    words.createTempView("words")

    val result: DataFrame = spark.sql("select value as word,count(1) as cnts from words group by value")

    result.show()

    spark.stop()
  }

}
