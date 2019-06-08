package com.wyd.spark.sparkdemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCountDemo {

  def main(args: Array[String]): Unit = {

    //创建spark执行入口
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://hdp12/wc")

    val sorted: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false)

    //保存到hdfs中
    sorted.saveAsTextFile("hdfs://hdp12/wcout")

    sc.stop()
  }

}
