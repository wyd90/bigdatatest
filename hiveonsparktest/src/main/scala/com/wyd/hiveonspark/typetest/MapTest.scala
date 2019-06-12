package com.wyd.hiveonspark.typetest

import org.apache.spark.sql.SparkSession

object MapTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MapTest")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_user(id int,name string,family_members map<string,string>,age int) " +
//      "row format delimited fields terminated by ',' collection items terminated by '#' " +
//      "map keys terminated by ':'")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/userdata/user.txt' into table t_user")

    spark.sql("select * from t_user").show()

    spark.sql("select id,name,age,family_members['brother'] brother from t_user where array_contains(map_keys(family_members), 'brother')").show()

    spark.stop()

  }

}
