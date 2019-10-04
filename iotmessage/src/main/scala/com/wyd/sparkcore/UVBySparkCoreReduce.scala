package com.wyd.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UVBySparkCoreReduce {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("UVByCoreReduce").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("file:////Users/wangyadi/yarnData/uv/input")

    val accessLog: RDD[(String, String, Int)] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      if (arr.length == 4) {
        val timeArr: Array[String] = arr(2).split(" ")
        if (timeArr.length == 2 && timeArr(0).equals("2017-09-16")) {
          (arr(0), arr(3), 1)
        } else {
          ("", "", 1)
        }
      } else {
        ("", "", 1)
      }
    })

    val filtered: RDD[(String, String, Int)] = accessLog.filter(_._1 != "")

    val cached: RDD[(String, String, Int)] = filtered.cache()

    val distincted = cached.distinct()

    val maped: RDD[(String, Int)] = distincted.map(x => (x._2, 1))
    val reduced: RDD[(String, Int)] = maped.reduceByKey(_+_)

    println(reduced.collect().toBuffer)

    val maped2 = cached.map(x => (x._2, 1))

    val grouped: RDD[(String, Iterable[Int])] = maped2.groupByKey()

    val pv: RDD[(String, Int)] = grouped.mapValues(it => {
      val count = it.toList.sum
      count
    })

    println(pv.collect().toBuffer)

    sc.stop()
  }

}
