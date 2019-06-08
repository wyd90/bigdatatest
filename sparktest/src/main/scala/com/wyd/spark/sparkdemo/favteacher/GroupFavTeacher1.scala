package com.wyd.spark.sparkdemo.favteacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher1")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/favt/input")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject: String = (splits(2).split("[.]")) (0)
      val teacher = splits(3)
      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)


    val result: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(it => {
      val list = it.toList
      val sorted = list.sortBy(_._2).reverse
      sorted
    })

    result.saveAsTextFile("sparkData/favt/out1")

    sc.stop()

  }


}
