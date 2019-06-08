package com.wyd.spark.sparkdemo.favteacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable



object GroupFavTeacher3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupFavTeacher3")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("sparkData/favt/input")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject: String = (splits(2).split("[.]")) (0)
      val teacher = splits(3)
      ((subject, teacher), 1)
    })


    //得到所有学科
    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    val partitioner = new TeacherAndSubjectPartitioner(subjects)
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(partitioner, (x:Int, y:Int) => x+y)

    implicit val teacherOrdering = new Ordering[((String,String), Int)] {
      override def compare(x: ((String, String), Int), y: ((String, String), Int)): Int = y._2 - x._2
    }

    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      val treeSet: mutable.TreeSet[((String, String), Int)] = new mutable.TreeSet[((String, String), Int)]()
      it.foreach(x => {
        treeSet.add(x)
        if (treeSet.size == 2) {
          val last: ((String, String), Int) = treeSet.last
          treeSet.remove(last)
        }
      })
      treeSet.toIterator
    })

    sorted.saveAsTextFile("sparkData/favt/out3")

    sc.stop()
  }

  class TeacherAndSubjectPartitioner(subjects: Array[String]) extends Partitioner {

    var rules = new mutable.HashMap[String,Int]()
    var i = 0;
    for(subject <- subjects){
      rules.put(subject,i)
      i += 1
    }

    override def numPartitions: Int = subjects.length

    override def getPartition(key: Any): Int = {
      val tuple = key.asInstanceOf[(String,String)]
      rules(tuple._1)
    }
  }

}
