package com.wyd.spark.sparkdemo.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort2").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val users: Array[String] = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 98","laoyang 28 99")

    val lines: RDD[String] = sc.parallelize(users)

    val userRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      (fields(0), fields(1).toInt, fields(2).toInt)
    })

    import SortRules.rule
    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(u => u)

    println(sorted.collect().toBuffer)

    sc.stop()
  }

}
