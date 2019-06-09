package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object FavTeacherSQLWithDSL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GroupFavTeacherSqlWithDSL").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("sparkData/favt/input")

    import spark.implicits._

    val subjectAndTeacher: DataFrame = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject: String = (splits(2).split("[.]")) (0)
      val teacher = splits(3)

      (subject, teacher)
    }).toDF("subject", "teacher")

    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = subjectAndTeacher.groupBy($"subject", $"teacher")
      .agg(count("*") as "cnts")
      .select($"subject", $"teacher",$"cnts",row_number().over (Window.partitionBy($"subject").orderBy($"cnts" desc)) as "rk")
      .select("*").where($"rk" <= 1)
    result.show()

    spark.stop()
  }

}
