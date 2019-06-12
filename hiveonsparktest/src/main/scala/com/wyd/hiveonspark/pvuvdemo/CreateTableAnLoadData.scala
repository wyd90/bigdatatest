package com.wyd.hiveonspark.pvuvdemo

import org.apache.spark.sql.SparkSession

object CreateTableAnLoadData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreateTableAnLoadData")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_web_log(ip string,uid string,access_time string,url string) " +
//      "partitioned by (day string) row format delimited fields terminated by ','")
//
//    spark.sql("load data inpath '/user/root/hiveonspark/pvuv/9-15.log' into table t_web_log partition(day='2017-09-15')")
//    spark.sql("load data inpath '/user/root/hiveonspark/pvuv/9-16.log' into table t_web_log partition(day='2017-09-16')")
//    spark.sql("load data inpath '/user/root/hiveonspark/pvuv/9-17.log' into table t_web_log partition(day='2017-09-17')")

    spark.sql("create table t_user_active_day(ip string,uid string,first_access string,url string) " +
      "partitioned by(day string) row format delimited fields terminated by ','")

    spark.sql("create table t_user_history(uid string)")

    spark.sql("create table t_user_new_day like t_user_active_day")

    spark.stop()
  }

}
