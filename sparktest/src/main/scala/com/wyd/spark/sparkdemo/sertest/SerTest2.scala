package com.wyd.spark.sparkdemo.sertest

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerTest2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/serTest/input")

    //rules在Driver端初始化，伴随着Task发送过去，每个Task有一个单独的实例
    val rules = new Rules

    val result: RDD[(String, String, Int, String)] = lines.flatMap(_.split(" ")).map(word => {
      val hostName: String = InetAddress.getLocalHost.getHostAddress
      val threadName: String = Thread.currentThread().getName

      (hostName, threadName, rules.rulesMap.getOrElse(word, 0), rules.toString)
    })
    result.saveAsTextFile("sparkData/serTest/out2")
    sc.stop()
  }

}
