package com.wyd.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sqlhbasetest").enableHiveSupport().getOrCreate()

    val df: DataFrame = spark.sql("select * from guanzhu")

    df.show();
  }

}
