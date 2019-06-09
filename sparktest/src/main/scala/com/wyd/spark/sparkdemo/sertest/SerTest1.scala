package com.wyd.spark.sparkdemo.sertest

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/serTest/input")

    val result: RDD[(String, String, Int, String)] = lines.flatMap(_.split(" ")).map(word => {
      //每个元素new一个新rules，很浪费资源
      val rules = new Rules
      val hostName: String = InetAddress.getLocalHost.getHostAddress
      val threadName: String = Thread.currentThread().getName

      (hostName, threadName, rules.rulesMap.getOrElse(word, 0), rules.toString)
    })
    result.saveAsTextFile("sparkData/serTest/out1")
    sc.stop()
  }

}
