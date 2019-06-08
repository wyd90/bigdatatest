package com.wyd.spark.sparkdemo.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users: Array[String] = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 98","laoyang 28 99")

    val userRDD: RDD[String] = sc.parallelize(users)

    val rdd = userRDD.map(userLine => {
      val fields: Array[String] = userLine.split(" ")
      val user = new User(fields(0), fields(1).toInt, fields(2).toInt)
      user
    })

    val sorted: RDD[User] = rdd.sortBy(u => u)

    println(sorted.collect().toBuffer)

    sc.stop()
  }

}

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if(that.fv - this.fv == 0){
      this.age - that.age
    } else {
      that.fv - this.fv
    }
  }

  override def toString: String = s"name:$name age:$age fv:$fv"
}
