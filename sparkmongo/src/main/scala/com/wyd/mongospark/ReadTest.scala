package com.wyd.mongospark

import com.mongodb.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object ReadTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("MongoSparkReadTest")
      .config("spark.mongodb.input.uri", "mongodb://admin:az63091919@192.168.56.101:27017,192.168.56.102:27017,192.168.56.103:27017/mobike.bikes?authSource=admin")
      .config("spark.mongodb.output.uri","mongodb://admin:az63091919@192.168.56.101:27017,192.168.56.102:27017,192.168.56.103:27017/mobike.bikescopy?authSource=admin")
      .getOrCreate()

    val document: DataFrame = MongoSpark.load(spark)

    document.printSchema()

    import spark.implicits._
    val xyTable: DataFrame = document.map(doc => {
      val doubles: mutable.WrappedArray[Double] = doc.getAs[mutable.WrappedArray[Double]]("location")
      (doubles(0), doubles(1))
    }).toDF("x", "y")

    xyTable.show()

    //MongoSpark.save(document)

    spark.stop()

  }

}
