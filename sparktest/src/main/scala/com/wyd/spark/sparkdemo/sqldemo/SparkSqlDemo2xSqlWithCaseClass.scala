package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlDemo2xSqlWithCaseClass {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksqldemo2xsqlwithcaseclass").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("sparkData/sql/people/input")

    import spark.implicits._
    val peopleSet: Dataset[People] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      People(id, name, age, fv)
    })

    val peopleFrame: DataFrame = peopleSet.toDF()

    peopleFrame.createTempView("people")

    val result: DataFrame = spark.sql("select id,name,age,fv from people where fv > 98 order by fv desc,age asc")

    result.show()

    result.write.parquet("sparkData/sql/people/out/parquet")

    spark.stop()
  }

}

case class People(id: Long, name: String, age: Int, fv: Double)
