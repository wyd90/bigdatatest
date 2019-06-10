package com.wyd.hiveonspark

import org.apache.spark.sql.SparkSession

object JoinTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hiveonsparkjoin").master("local[*]").enableHiveSupport().getOrCreate()

    //System.setProperty("HADOOP_USER_NAME","root")
    spark.sql("create table t_a(name string,numb int) " +
      "row format delimited " +
      "fields terminated by ','")

    spark.sql("create table t_b(name string,nick string) " +
      "row format delimited " +
      "fields terminated by ','")

    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/jointest/input/a.txt' into table t_a")
    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/jointest/input/b.txt' into table t_b")

    spark.stop()
  }

}
