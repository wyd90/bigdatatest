package com.wyd.hiveonspark

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hiveonsparkjoin").master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_ratingjson(json string)")
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/ratingdata/rating.json' into table t_ratingjson")

    //spark.sql("select * from t_ratingjson").show()

    spark.udf.register("jsonpa",(json: String,col: String) =>{
        val obj: JSONObject = JSON.parseObject(json)
        obj.getString(col)
    })

    spark.sql("select jsonpa(json,'movie') movie,cast(jsonpa(json,'rate') as INT) rate,from_unixtime(cast(jsonpa(json,'timeStamp') as BIGINT),'yyyy/MM/dd HH:mm:ss') time,jsonpa(json,'uid') uid from t_ratingjson limit 10").show()

    val tmpDF: DataFrame = spark.sql("select json_tuple(json,'movie','rate','timeStamp','uid') as (movie,rate,ts,uid) from t_ratingjson limit 10")

    tmpDF.createTempView("tmp")

    spark.sql("select movie,cast(rate as INT),from_unixtime(cast(ts as BIGINT),'yyyy/MM/dd HH:mm:ss') time,uid from tmp").show()

    spark.stop()
  }

}

