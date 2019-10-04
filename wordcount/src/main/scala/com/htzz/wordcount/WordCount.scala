package com.htzz.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {

  def main(args: Array[String]): Unit = {
    println("testwordcount starting ================================================")
    //System.setProperty("HADOOP_USER_NAME","root");
    //创建spark执行入口
    val conf = new SparkConf().setAppName("testwordcount")//.set("spark.driver.port","50003").set("spark.blockManager.port","50004").set("spark.port.maxRetries","5")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://amb-agt-1:8020/wc")

    val sorted: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false)

    //保存到hdfs中
    sorted.saveAsTextFile("hdfs://amb-agt-1:8020/wcoutspark0719")

    sc.stop()

  }

}
