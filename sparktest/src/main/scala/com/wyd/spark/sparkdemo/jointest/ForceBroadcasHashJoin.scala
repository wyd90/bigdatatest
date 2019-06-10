package com.wyd.spark.sparkdemo.jointest

import org.apache.spark.sql.{DataFrame, SparkSession}

object ForceBroadcasHashJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("broadcashashjoinexample").master("local[*]").getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",1024*1024*10)

    import spark.implicits._


    val df1: DataFrame = Seq((0, "playing"), (1, "with"), (2, "join")).toDF("id", "token")

    val df2: DataFrame = Seq((0, "P"), (1, "W"), (2, "J")).toDF("aid", "atoken")

    val result: DataFrame = df1.join(df2, $"id" === $"aid", "left_outer")

    result.show()

    result.explain(true)

    spark.stop()
  }
}
