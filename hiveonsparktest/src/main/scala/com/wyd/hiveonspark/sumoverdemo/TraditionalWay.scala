package com.wyd.hiveonspark.sumoverdemo

import org.apache.spark.sql.SparkSession

object TraditionalWay {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TraditionalWay")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_accumulate(uid string,month string,amount int) " +
//      "row format delimited fields terminated by ','")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/accumulate/accumulate.txt' into table t_accumulate")


//    spark.sql("create table t_accumulate_mid as " +
//      "select uid,month,sum(amount) samount from t_accumulate group by uid,month")

    spark.sql("select * from t_accumulate_mid").show()

    spark.sql("select uid,month,amount," +
      "sum(amount) over(partition by uid order by month rows between unbounded preceding and current row) accumulate" +
      " from (select uid,month,sum(amount) amount from t_accumulate group by uid,month) tmp").show()

    spark.stop()
  }

}
