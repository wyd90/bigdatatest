package com.wyd.spark.sparkdemo.favteacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupFavTeacher2")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/favt/input")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject: String = (splits(2).split("[.]")) (0)
      val teacher = splits(3)
      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)
    //得到所有学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    //将数据缓存到executor中
    val cached: RDD[((String, String), Int)] = reduced.persist()

    for(subject <- subjects){
      val filtered: RDD[((String, String), Int)] = cached.filter(_._1._1.equals(subject))
      val result: RDD[((String, String), Int)] = filtered.sortBy(_._2,false)

      //println(result.collect().toBuffer)
      result.saveAsTextFile("sparkData/favt/out2/"+subject+"out")
    }

    sc.stop()

  }
}
