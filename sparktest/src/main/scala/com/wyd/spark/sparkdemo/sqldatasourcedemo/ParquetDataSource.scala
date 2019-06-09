package com.wyd.spark.sparkdemo.sqldatasourcedemo

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("parquetDataSource").getOrCreate()

    val peoples: DataFrame = spark.read.parquet("sparkData/sql/people/out/parquet")

    peoples.printSchema()

    peoples.show()

    spark.stop()
  }

}
