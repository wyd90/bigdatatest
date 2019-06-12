package com.wyd.hiveonspark.pvuvdemo

import org.apache.spark.sql.SparkSession

object SatisticPVUV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SatisticPVUV")
      .config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .enableHiveSupport().getOrCreate()

    spark.sql("insert into table t_user_active_day partition(day='2017-09-15') " +
      "select tmp.ip ip,tmp.uid uid,tmp.access_time first_access,tmp.url url from " +
      "(select ip,uid,access_time,url,row_number() over(partition by uid order by access_time) rn " +
      "from t_web_log where day='2017-09-15') tmp  where rn < 2")

    spark.sql("insert into table t_user_new_day partition(day='2017-09-15') " +
      "select tuad.ip ip,tuad.uid uid,tuad.first_access first_access,tuad.url url from " +
      "t_user_active_day tuad left outer join t_user_history tuh on tuad.uid = tuh.uid " +
      "where tuad.day = '2017-09-15' and tuh.uid IS NULL")

    spark.sql("insert into table t_user_history select uid from t_user_new_day " +
      "where day='2017-09-15'")

    spark.sql("insert into table t_user_active_day partition(day='2017-09-16') " +
      "select tmp.ip ip,tmp.uid uid,tmp.access_time first_access,tmp.url url from " +
      "(select ip,uid,access_time,url,row_number() over(partition by uid order by access_time) rn " +
      "from t_web_log where day='2017-09-16') tmp  where rn < 2")

    spark.sql("insert into table t_user_new_day partition(day='2017-09-16') " +
      "select tuad.ip ip,tuad.uid uid,tuad.first_access first_access,tuad.url url from " +
      "t_user_active_day tuad left outer join t_user_history tuh on tuad.uid = tuh.uid " +
      "where tuad.day = '2017-09-16' and tuh.uid IS NULL")

    spark.sql("insert into table t_user_history select uid from t_user_new_day " +
      "where day='2017-09-16'")

    spark.sql("insert into table t_user_active_day partition(day='2017-09-17') " +
      "select tmp.ip ip,tmp.uid uid,tmp.access_time first_access,tmp.url url from " +
      "(select ip,uid,access_time,url,row_number() over(partition by uid order by access_time) rn " +
      "from t_web_log where day='2017-09-17') tmp  where rn < 2")

    spark.sql("insert into table t_user_new_day partition(day='2017-09-17') " +
      "select tuad.ip ip,tuad.uid uid,tuad.first_access first_access,tuad.url url from " +
      "t_user_active_day tuad left outer join t_user_history tuh on tuad.uid = tuh.uid " +
      "where tuad.day = '2017-09-17' and tuh.uid IS NULL")

    spark.sql("insert into table t_user_history select uid from t_user_new_day " +
      "where day='2017-09-17'")

    spark.stop()
  }

}
