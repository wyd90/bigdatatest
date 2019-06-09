package com.wyd.spark.sparkdemo.sparksqljoinandudfudaf

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china","2,laoduan,usa"))

    val userDF: DataFrame = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)

      (id, name, nationCode)
    }).toDF("id","name","nationCode")

    val nationDataSet: Dataset[String] = spark.createDataset(List("china,中国","usa,美国"))

    val nationDF: DataFrame = nationDataSet.map(data => {
      val fields: Array[String] = data.split(",")
      val ncode = fields(0)
      val nname = fields(1)

      (ncode, nname)
    }).toDF("ncode", "nname")

    //sql风格
//    userDF.createTempView("user")
//    nationDF.createTempView("nation")
//    val result: DataFrame = spark.sql("select u.id,u.name,n.nname as nationName from user u left join nation n on u.nationCode = n.ncode")

    //DSL风格
    val result: DataFrame = userDF.join(nationDF,$"nationCode" === $"ncode", "left_outer").select("id","name","nname")

    result.show()

    spark.stop()
  }

}
