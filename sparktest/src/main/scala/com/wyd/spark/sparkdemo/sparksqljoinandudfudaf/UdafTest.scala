package com.wyd.spark.sparkdemo.sparksqljoinandudfudaf

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UdafTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("udaftest").master("local[*]").getOrCreate()

    import spark.implicits._
    val range: Dataset[Int] = spark.createDataset(1 to 11)
    val geomean = new GeoMean

    val rangeDF = range.toDF("value")

    //sql风格
//    rangeDF.createTempView("range")
//    spark.udf.register("gm",geomean)
//    val result: DataFrame = spark.sql("select gm(value) from range")

    //DSL风格
    val result: DataFrame = rangeDF.agg(geomean($"value") as "v")
    result.show()

    spark.stop()

  }

}

class GeoMean extends UserDefinedAggregateFunction{

  //输入的数据类型
  override def inputSchema: StructType = StructType(List(StructField("value",DoubleType)))

  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(
    StructField("counts", LongType),
    StructField("product", DoubleType)
  ))

  //最终返回结果的类型
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //参与运算数字个数的初始值
    buffer(0) = 0L
    //参与相乘的初始值
    buffer(1) = 1.0
  }

  //每有一条数据参与运算就更新一下中间结果（update相当于在每一个分区中的运算）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //参与运算的个数更新
    buffer(0) = buffer.getLong(0) + 1L
    //每有一个数字参与运算就进行相乘（包含中间结果）
    buffer(1) = buffer.getDouble(1) * input.getDouble(0)
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区参与运算的中间结果进行相加
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //每个分区计算的结果进行相乘
    buffer1(1) = buffer1.getDouble(1) * buffer2.getDouble(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    Math.pow(buffer.getDouble(1), 1.toDouble/buffer.getLong(0))
  }
}
