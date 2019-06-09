package com.wyd.spark.sparkdemo.sqldatasourcedemo

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jdbcDataSource").getOrCreate()

    val props = new Properties()
    props.put("user","root")
    props.put("password","az63091919")
    val peoples: DataFrame = spark.read
      .options(Map("driver" -> "com.mysql.jdbc.Driver"))
      .jdbc("jdbc:mysql://192.168.56.103:3306/bigdata?useUnicode=true&characterEncoding=utf8","people",props)

    peoples.printSchema()

    peoples.show()

    spark.stop()
  }

}
