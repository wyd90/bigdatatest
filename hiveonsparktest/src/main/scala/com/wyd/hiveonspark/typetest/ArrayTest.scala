package com.wyd.hiveonspark.typetest

import org.apache.spark.sql.{Dataset, SparkSession}

object ArrayTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("arrTest")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_movie(movie_name string, actors array<string>, first_show date) " +
//      "row format delimited fields terminated by ',' collection items terminated by ':'")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/moviedata/movie.txt' into table t_movie")

    spark.sql("select * from t_movie").show()

    spark.sql("select * from t_movie where array_contains(actors,'龙母')").show()

    spark.sql("select movie_name,actors,first_show,size(actors) from t_movie").show()
    spark.stop()
  }

}
