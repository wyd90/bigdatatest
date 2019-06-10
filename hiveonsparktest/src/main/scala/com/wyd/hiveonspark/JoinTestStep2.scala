package com.wyd.hiveonspark

import org.apache.spark.sql.SparkSession

object JoinTestStep2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hiveonsparkjoin").master("local[*]").enableHiveSupport().getOrCreate()

    //内连接
    spark.sql("select a.name aname,a.numb,b.name bname,b.nick from t_a a inner join t_b b on a.name = b.name").show()

    //左外连接
    spark.sql("select a.name aname,a.numb,b.name bname,b.nick from t_a a left outer join t_b b on a.name = b.name").show()

    //右外连接
    spark.sql("select a.name aname,a.numb,b.name bname,b.nick from t_a a right outer join t_b b on a.name = b.name").show()

    //全外连接
    spark.sql("select a.name aname,a.numb,b.name bname,b.nick from t_a a full outer join t_b b on a.name = b.name").show()

    //左半连接(只返回左表的数据)
    spark.sql("select a.* from t_a a left semi join t_b b on a.name = b.name").show()

    spark.stop()
  }

}
