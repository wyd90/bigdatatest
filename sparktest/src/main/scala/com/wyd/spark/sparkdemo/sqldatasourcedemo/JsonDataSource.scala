package com.wyd.spark.sparkdemo.sqldatasourcedemo

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jsonDataSource").getOrCreate()

    val peoples: DataFrame = spark.read.json("sparkData/sql/people/out/json")

    peoples.printSchema()

    peoples.show()

    spark.stop()
  }

}
