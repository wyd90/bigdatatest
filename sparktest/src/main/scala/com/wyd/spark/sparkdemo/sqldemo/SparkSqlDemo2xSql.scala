package com.wyd.spark.sparkdemo.sqldemo

import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlDemo2xSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSql2xsql").getOrCreate()

    val peopleSet: Dataset[String] = spark.read.textFile("sparkData/sql/people/input")

    import spark.implicits._
    val frame: DataFrame = peopleSet.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble

      (id, name, age, fv)
    }).toDF("id", "name", "age", "fv")


    frame.createTempView("people")

    val result: DataFrame = spark.sql("select id,name,age,fv from people where fv > 98 order by fv desc,age asc")

    result.show()


    val props = new Properties()
    props.put("user","root")
    props.put("password","az63091919")

    result.write.mode("ignore")
      .options(Map("driver" -> "com.mysql.jdbc.Driver"))
      .jdbc("jdbc:mysql://192.168.56.103:3306/bigdata?useUnicode=true&characterEncoding=utf8","people",props)

    spark.stop()
  }

}
