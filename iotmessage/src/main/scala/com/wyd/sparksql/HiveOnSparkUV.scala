package com.wyd.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSparkUV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().master("local[*]").appName("HiveOnSparkUV").getOrCreate()

    val frame1: DataFrame = spark.sql("select url,count(1),count(distinct(ip)) from t_access group by day,url having day='2017-09-16'")
    frame1.show()

    spark.stop()

  }

}
