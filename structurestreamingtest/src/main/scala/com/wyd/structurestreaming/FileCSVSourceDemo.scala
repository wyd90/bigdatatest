package com.wyd.structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object FileCSVSourceDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("filecsvsourcedemo")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val userSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("sex", StringType)
    ))

    //如果读的文件夹中有子目录，并且，子目录名字是year=2018这种key=value形式的
    //就会在frame后面加一列 year，相当于hive的分区表
    val frame = spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("file:///Users/wangyadi/yarnData/peoplecsv")

    frame.createOrReplaceTempView("t_people")

    val res = sql("select sex,avg(age) avgage from t_people group by sex")

    res.writeStream
      .outputMode("complete")
      .format("console")
      .start().awaitTermination()



  }

}
