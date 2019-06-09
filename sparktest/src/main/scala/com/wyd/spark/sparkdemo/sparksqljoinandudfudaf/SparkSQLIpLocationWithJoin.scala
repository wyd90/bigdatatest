package com.wyd.spark.sparkdemo.sparksqljoinandudfudaf

import java.util.Properties

import com.wyd.spark.sparkdemo.broadcastdemo.IpUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLIpLocationWithJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksqliplocationwithjoin").getOrCreate()

    val ruleLines: Dataset[String] = spark.read.textFile("sparkData/ipLocation/rules")

    import spark.implicits._
    val ruleDF: DataFrame = ruleLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      (fields(2).toLong, fields(3).toLong, fields(6))
    }).toDF("startIp", "endIp", "cityName")

    val accessSet: Dataset[String] = spark.read.textFile("sparkData/ipLocation/input")

    val ipDF = accessSet.map(line => {
      val fields: Array[String] = line.split("[|]")

      val ip = IpUtil.ip2Long(fields(1))

      ip
    }).toDF("ip")


    //sql风格
//    ipDF.createTempView("v_ip")
//    ruleDF.createTempView("v_rules")
//    val v_tem1: DataFrame = spark.sql("select v_rules.cityName,count(*) as cnts from v_ip left join v_rules on (v_ip.ip >= v_rules.startIp and v_ip.ip <= v_rules.endIp) group by v_rules.cityName")
//    v_tem1.createTempView("v_tem1")
//    val result: DataFrame = spark.sql("select * from v_tem1 order by cnts desc")

    //DSL风格
    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = ipDF.join(ruleDF, $"ip" >= $"startIp" and $"ip" <= $"endIp", "left_outer")
      .groupBy($"cityName").agg(count("*") as "cnts")
      .orderBy($"cnts" desc)


    result.show()
    
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","az63091919")
    result.write.mode("ignore")
      .options(Map("driver" -> "com.mysql.jdbc.Driver"))
      .jdbc("jdbc:mysql://192.168.56.103:3306/bigdata?useUnicode=true&characterEncoding=utf8","access",props)

    spark.stop()

  }

}
