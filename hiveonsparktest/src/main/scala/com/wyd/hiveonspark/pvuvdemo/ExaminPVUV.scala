package com.wyd.hiveonspark.pvuvdemo

import org.apache.spark.sql.SparkSession

object ExaminPVUV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreateTableAnLoadData")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

    spark.sql("select * from t_user_active_day where day='2017-09-15'").show()
    spark.sql("select * from t_user_active_day where day='2017-09-16'").show()
    spark.sql("select * from t_user_active_day where day='2017-09-17'").show()

    spark.sql("select * from t_user_new_day where day='2017-09-15'").show()
    spark.sql("select * from t_user_new_day where day='2017-09-16'").show()
    spark.sql("select * from t_user_new_day where day='2017-09-17'").show()

    spark.sql("select * from t_user_history").show()

    spark.stop()

  }

}
