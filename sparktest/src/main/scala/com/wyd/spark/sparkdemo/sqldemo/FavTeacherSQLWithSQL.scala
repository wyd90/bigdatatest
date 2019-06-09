package com.wyd.spark.sparkdemo.sqldemo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FavTeacherSQLWithSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GroupFavTeacherSqlWithSql").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("sparkData/favt/input")

    import spark.implicits._

    val subjectAndTeacher: DataFrame = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject: String = (splits(2).split("[.]")) (0)
      val teacher = splits(3)

      (subject, teacher)
    }).toDF("subject", "teacher")

    subjectAndTeacher.createTempView("subAndTeacher")

    val tem_v1: DataFrame = spark.sql("select subject,teacher,count(*) as cnts from subAndTeacher group by subject,teacher")
    tem_v1.createTempView("tem_v1")

    val tem_v2: DataFrame = spark.sql("select subject,teacher,cnts,row_number() over(partition by subject order by cnts desc) as rk from tem_v1")
    tem_v2.createTempView("tem_v2")

    val result = spark.sql("select * from tem_v2 where rk <= 1")

    result.show()

    spark.stop()


  }

}
