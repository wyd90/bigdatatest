package com.wyd.spark.sparkdemo.broadcastdemo

import java.sql.{Connection, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("sparkData/ipLocation/rules")
    val rulesRdd: RDD[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      (fields(2).toLong, fields(3).toLong, fields(6))
    })

    val rules = rulesRdd.collect()

    val broadcastRules: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    val dataLines = sc.textFile("sparkData/ipLocation/input")

    val locations: RDD[(String, Int)] = dataLines.map(dataLine => {
      val fields: Array[String] = dataLine.split("[|]")
      val ip: String = fields(1)

      val ipLong: Long = IpUtil.ip2Long(ip)
      val index: Int = IpUtil.binarySearch(broadcastRules.value, ipLong)
      var province = "未知"
      if (index != -1) {
        province = (broadcastRules.value) (index)._3
      }
      (province, 1)
    })
    val reduced: RDD[(String, Int)] = locations.reduceByKey(_+_)

    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    //println(sorted.collect().toBuffer)


    sorted.foreachPartition(it => {
      Class.forName("com.mysql.jdbc.Driver")
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://node3:3306/bigdata?useUnicode=true&characterEncoding=utf8","root","az63091919")
      val pstm = conn.prepareStatement("insert into access (province,cnts) value(?,?)")
      it.foreach(tp =>{
        pstm.setString(1,tp._1)
        pstm.setInt(2,tp._2)
        pstm.executeUpdate()
      })
      pstm.close()
      conn.close()
    })
    sc.stop()
  }
}
