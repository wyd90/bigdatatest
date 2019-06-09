package com.wyd.spark.sparkdemo.sparksqljoinandudfudaf

import com.wyd.spark.sparkdemo.broadcastdemo.IpUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLIpLocationWithUdf {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksqliplocationwithUdf")getOrCreate()

    val ruleLines: Dataset[String] = spark.read.textFile("sparkData/ipLocation/rules")

    import spark.implicits._
    val rulesSet: Dataset[(Long, Long, String)] = ruleLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      (fields(2).toLong, fields(3).toLong, fields(6))
    })

    val rules: Array[(Long, Long, String)] = rulesSet.collect()

    val rulesRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rules)

    spark.udf.register("ip2Province", (ip: Long) => {
      val index: Int = IpUtil.binarySearch(rulesRef.value, ip)
      var province = "未知"
      if(index != -1) {
        province = (rulesRef.value)(index)._3
      }
      province
    })

    val accessSet: Dataset[String] = spark.read.textFile("sparkData/ipLocation/input")

    val ipDF = accessSet.map(line => {
      val fields: Array[String] = line.split("[|]")

      val ip = IpUtil.ip2Long(fields(1))

      ip
    }).toDF("ip")

    //sql风格
//    ipDF.createTempView("v_ip")
//    val v_tmp1: DataFrame = spark.sql("select ip2Province(ip) as cityName from v_ip")
//    v_tmp1.createTempView("v_tmp1")
//    val result: DataFrame = spark.sql("select cityName,count(*) as cnts from v_tmp1 group by cityName order by cnts desc")

    //DSL风格
    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = ipDF.select(callUDF("ip2Province", $"ip") as "cityName")
      .groupBy($"cityName").agg(count("*") as "cnts")
      .orderBy($"cnts" desc)

    result.show()

    spark.stop()
  }

}
