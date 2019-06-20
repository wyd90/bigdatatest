package com.wyd.spark.scalademo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("coalesceTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello world jack","hi tom marry hank","mydog is doudou","hay hank tom world"),4)

    println(s"rdd1 = ${rdd1.partitions.length}")

    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_,1))

    println(s"rdd2 = ${rdd2.partitions.length}")

    val rdd3 = rdd2.coalesce(2)

    println(s"rdd3 = ${rdd3.partitions.length}")

    val rdd4 = rdd3.repartition(5)

    println(s"rdd4 = ${rdd4.partitions.length}")


  }


}
