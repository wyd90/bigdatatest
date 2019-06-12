package com.wyd.hiveonspark.explodedemo

import org.apache.spark.sql.SparkSession

object ExplodeDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExplodeDemo")
      //.config("spark.sql.warehouse.dir","hdfs://hdp12/user/hive2/warehouse")
      .master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_stu_subject(id int,name string,subjects array<string>) " +
//      "row format delimited fields terminated by ',' collection items terminated by ':'")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/subjects/subjects.txt' into table t_stu_subject")

    spark.sql("select explode(subjects) from t_stu_subject").show()

    spark.sql("select distinct tmp.subs from (select explode(subjects) subs from t_stu_subject) tmp").show()

    spark.sql("select id,name,sub from t_stu_subject lateral view explode(subjects) tmp_view as sub").show()

    spark.stop()
  }

}
