package com.wyd.spark.sparkdemo.sertest

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerTest4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/serTest/input")

    val result: RDD[(String, String, Int, String)] = lines.flatMap(_.split(" ")).map(word => {
      val hostName: String = InetAddress.getLocalHost.getHostAddress
      val threadName: String = Thread.currentThread().getName
      //rules在Executor中初始化，并且只有一个实例，多个Task共享
      (hostName, threadName, ExecutorRules.rulesMap.getOrElse(word, 0), ExecutorRules.toString)
    })
    result.saveAsTextFile("sparkData/serTest/out4")
    sc.stop()
  }

}
