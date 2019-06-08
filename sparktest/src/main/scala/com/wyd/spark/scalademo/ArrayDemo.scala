package com.wyd.spark.scalademo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object ArrayDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ArrayDemo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //将一个集合通过并行化变成RDD
    val arr = Array(1,2,3,4,5,6,7,8,9,10)
    val rdd1: RDD[Int] = sc.parallelize(arr)

    val rdd2: RDD[Int] = sc.parallelize(List(10,11,12,13,14,15))

    //求两个集合的并集
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    println(rdd3.collect().toBuffer)

    //求两个集合的交集
    println(rdd1.intersection(rdd2).collect().toBuffer)


    val rdd4: RDD[(String, Int)] = sc.parallelize(List(("tom",1),("jerry",2),("kitty",3)))
    val rdd5: RDD[(String, Int)] = sc.parallelize(List(("jerry",9),("tom",8),("shuke",7),("tom",2)))

    //join（连接）方法,join只能按照key等值join
    println(rdd4.join(rdd5).collect().toBuffer)

    //leftOuterJoin 左外连接，左表的数据一定显示，右表的数据有就显示，没有就不显示
    println(rdd4.leftOuterJoin(rdd5).collect().toBuffer)

    //rightOuterJoin 右外连接，右表的数据一定显示，左表的数据有就显示，没有就不显示
    val rdd6: RDD[(String, (Option[Int], Int))] = rdd4.rightOuterJoin(rdd5)
    println(rdd6.collect().toBuffer)

    //斜分组
    /**
      * 得Array((kitty,(CompactBuffer(3),CompactBuffer())), (tom,(CompactBuffer(1),CompactBuffer(8, 2))), (jerry,(CompactBuffer(2),CompactBuffer(9))), (shuke,(CompactBuffer(),CompactBuffer(7))))
      */
    val rdd7: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd4.cogroup(rdd5)
    println(rdd7.collect().toBuffer)

    //笛卡尔积
    //得
    //Array(((tom,1),(jerry,9)), ((tom,1),(tom,8)), ((tom,1),(shuke,7)), ((tom,1),(tom,2)), ((jerry,2),(jerry,9)), ((jerry,2),(tom,8)), ((jerry,2),(shuke,7)), ((jerry,2),(tom,2)), ((kitty,3),(jerry,9)), ((kitty,3),(tom,8)), ((kitty,3),(shuke,7)), ((kitty,3),(tom,2)))
    val rdd8: RDD[((String, Int), (String, Int))] = rdd4.cartesian(rdd5)

    println(rdd8.collect().toBuffer)

    //获取rdd分区的数量
    println(rdd4.partitions.length)

    //mapPartitionsWithIndex 一次拿出一个分区（分区中并没有数据，而是记录要读取哪些数据，真正生成的Task会读取多条数据），并且可以将分区的编号取出来
    val func = (index: Int, it: Iterator[Int]) => it.map(e => s"part: $index, ele: $e")
    val rdd9: RDD[String] = rdd1.mapPartitionsWithIndex(func)

    println(rdd9.collect().toBuffer)

    //要按key相加,注意：初始值只会在局部聚合中使用，不会在全局聚合中使用
    val rdd10: RDD[(String, Int)] = sc.parallelize(List(("cat",2),("cat",5),("mouse",4),("dog",12),("mouse",2)))
    println(rdd10.aggregateByKey(0)(_+_,_+_).collect().toBuffer)

    println(rdd1.aggregate(0)(math.max(_,_),_+_))

    //统计的是key出现的次数，和value没关系
    println(rdd10.countByKey().toBuffer)

    //按key过滤，得ArrayBuffer((cat,2), (cat,5))
    val rdd11: RDD[(String, Int)] = rdd10.filterByRange("a","d")
    println(rdd11.collect().toBuffer)

    val rdd12: RDD[String] = sc.parallelize(List("dog","wolf","cat","bear"),2)
    val rdd13: RDD[(Int, String)] = rdd12.map(x => (x.length,x))

    val rdd14: RDD[(Int, String)] = rdd13.foldByKey("")(_+_)
    println(rdd14.collect().toBuffer)

    //foreach一条一条拿，foreachPartition一次拿一个分区
    rdd1.foreachPartition(it => {
      it.foreach(x => println(x * 100))
    })

    //拉链操作 zip
    //得到ArrayBuffer((dog,1), (cat,1), (gnu,2), (salmon,2), (rabbit,2), (turkey,1), (wolf,2), (bear,2), (bee,2))
    val rdd15 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3)
    val rdd16 = sc.parallelize(List(1,1,2,2,2,1,2,2,2),3)
    val rdd17: RDD[(String, Int)] = rdd15.zip(rdd16)
    println(rdd17.collect().toBuffer)

    val rdd18 = sc.parallelize(Array((1,"dog"),(1,"cat"),(2,"gnu"),(2,"salmon"),(2,"rabbit"),(1,"turkey"),(2,"wolf"),(2,"bear"),(2,"bee")),2)

    import scala.collection.mutable.ListBuffer
    val rdd19: RDD[(Int, ListBuffer[String])] = rdd18.combineByKey(x => ListBuffer(x), (m: ListBuffer[String], n: String) => m += n, (a: ListBuffer[String], b: ListBuffer[String]) => a ++= b)

    println(rdd19.collect().toBuffer)

    sc.stop()
  }

}
