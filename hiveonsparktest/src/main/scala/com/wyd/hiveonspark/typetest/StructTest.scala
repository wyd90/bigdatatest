package com.wyd.hiveonspark.typetest

import org.apache.spark.sql.SparkSession

object StructTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StructTest")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_people(id int,name string,info struct<age:int,sex:string,addr:string>) " +
//      "row format delimited fields terminated by ',' collection items terminated by ':'")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/peoplestructtest' into table t_people")

    spark.sql("select id,name,info.addr from t_people").show()

    spark.stop()

  }

}
