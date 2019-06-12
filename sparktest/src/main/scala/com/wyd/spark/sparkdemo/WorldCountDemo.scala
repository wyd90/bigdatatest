package com.wyd.spark.sparkdemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCountDemo {

  def main(args: Array[String]): Unit = {

    //创建spark执行入口
    val conf = new SparkConf().set("spark.driver.port","50003").set("spark.blockManager.port","50004").set("spark.port.maxRetries","5")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/wc")

    val sorted: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false)

    //保存到hdfs中
    sorted.saveAsTextFile("/wcoutspark0611")

    sc.stop()
  }

}
