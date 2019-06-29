package com.wyd.spark.ml

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMLDemo1 {
  def main(args: Array[String]): Unit = {
    //数据目录
    var dataDir = "/Users/wangyadi/sparkml/wine/train";
    val spark = SparkSession.builder().appName("wineQuailty").master("local[*]").getOrCreate()
    val lines = spark.sparkContext.textFile(dataDir)

    val datas: RDD[Array[String]] = lines.map(_.split(";"))
    val wines: RDD[Wine] = datas.map(data => Wine(data(0).toDouble,data(1).toDouble,data(2).toDouble,data(3).toDouble,data(4).toDouble,data(5).toDouble,data(6).toDouble,data(7).toDouble,data(8).toDouble,data(9).toDouble,data(10).toDouble,data(11).toDouble))

    import spark.implicits._
    val trainDf: DataFrame = wines.map(wine => (wine.Quality, Vectors.dense(wine.FixedAcidity, wine.VolatileAcidity, wine.CitricAcid, wine.ResidualSugar, wine.Chlorides, wine.FreeSulfurDioxide, wine.TotalSulfurDioxide, wine.Density, wine.PH, wine.Sulphates, wine.Alcohol))).toDF("label", "features")

    trainDf.show()

    //创建线性回归对象
    val lr = new LinearRegression()
    //设置最大迭代次数
    lr.setMaxIter(100)
    //通过线性回归拟合训练数据，生成模型
    val model = lr.fit(trainDf)

    val testDF = spark.createDataFrame(Seq(
      (5.0, Vectors.dense(7.4, 0.24, 0.29, 10.1, 0.05, 21, 105, 0.9962, 3.13, 0.35, 9.5)),
      (7.0, Vectors.dense(7.1, 0.18, 0.36, 1.4, 0.043, 31, 87, 0.9898, 3.26, 0.37, 12.7)),
      (6.0, Vectors.dense(6.4, 0.33, 0.31, 5.5, 0.048, 42, 173, 0.9951, 3.19, 0.66, 9.3))
    )).toDF("label", "features")

    val tDF: DataFrame = testDF.select("features")

    val result: DataFrame = model.transform(tDF).select("features", "prediction")

    result.show()

  }
}

case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)

