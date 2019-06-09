package com.wyd.spark.sparkdemo.sertest

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerTest3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/serTest/input")
    //初始化object，在Driver端初始化，伴随着Task发送到Executor中，每个Executor中只有一个rules实例，多个Task共享
    val rules = StaticRules
    val result: RDD[(String, String, Int, String)] = lines.flatMap(_.split(" ")).map(word => {
      val hostName: String = InetAddress.getLocalHost.getHostAddress
      val threadName: String = Thread.currentThread().getName

      (hostName, threadName, rules.rulesMap.getOrElse(word, 0), rules.toString)
    })
    result.saveAsTextFile("sparkData/serTest/out3")
    sc.stop()
  }

}
