package com.wyd.hiveonspark

import org.apache.spark.sql.SparkSession

object pvLogTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hiveonsparkjoin").master("local[*]").enableHiveSupport().getOrCreate()

//    spark.sql("create table t_access(ip string,url string,access_time string) " +
//      "partitioned by(dt string) row format delimited " +
//      "fields terminated by ','")
//
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/accessdata/access.log.0804' into table t_access partition(dt='2017-08-04')")
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/accessdata/access.log.0805' into table t_access partition(dt='2017-08-05')")
//    spark.sql("load data inpath 'hdfs://hdp12/user/root/hiveonspark/accessdata/access.log.0806' into table t_access partition(dt='2017-08-06')")


    spark.sql("show partitions t_access").show()

    spark.sql("select count(1) cnts,max(ip),dt from t_access where url = 'http://www.edu360.cn/job' group by dt having dt > '2017-08-04'").show()

    spark.sql("select count(1) cnts,url,max(ip) ip,dt from t_access group by dt,url having dt > '2017-08-04' order by cnts desc").show()

    spark.sql("select a.* from (select url,max(ip) ip,dt,count(1) cnts from t_access group by dt,url having dt > '2017-08-04') a where a.cnts > 2").show()


    spark.sql("select url,max(ip) ip,dt,count(1) cnts from t_access group by dt,url having dt > '2017-08-04' and cnts > 2")

    //spark.sql("drop table t_access")

    spark.stop()

  }

}
