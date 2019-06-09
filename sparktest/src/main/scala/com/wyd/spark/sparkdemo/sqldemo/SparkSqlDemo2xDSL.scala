package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlDemo2xDSL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksql2xdsl").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("sparkData/sql/people/input")

    import spark.implicits._
    val peopleFrame: DataFrame = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble

      (id, name, age, fv)
    }).toDF("id", "name", "age", "fv")

    val result: Dataset[Row] = peopleFrame.select("id","name","age","fv")
      .where($"fv" > 98)
      .orderBy($"fv" desc,$"age" asc)

    result.show()

    result.write.json("sparkData/sql/people/out/json")

    spark.stop()
  }

}
