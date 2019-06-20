package com.wyd.spark.scalademo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PipeDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("pipDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("/Users/wangyadi"))

    val rdd2: RDD[String] = rdd.pipe("ls")

    println(rdd2.collect().toBuffer)
  }

}
