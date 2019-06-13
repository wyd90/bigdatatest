package com.wyd.ordersatistic

import com.wyd.redisutil.JedisClusterConnectPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.JedisCluster

object CalculateUtils {

  /**
    * 计算总成交金额
    * @param orders
    * @return
    */
  def calculateIncome(orders: RDD[Array[String]]) = {
    val money: RDD[Double] = orders.map(_(4).toDouble)
    val sum: Double = money.reduce(_+_)
    val conn: JedisCluster = JedisClusterConnectPool.getConnection()
    conn.incrByFloat("orderSum",sum)
  }

  def calculateByItem(orders: RDD[Array[String]]) = {
    val itemAndMoney = orders.map(order => {
      (order(2), order(4).toDouble)
    })
    val reduced: RDD[(String, Double)] = itemAndMoney.reduceByKey(_+_)

    reduced.foreachPartition(it => {
      val conn = JedisClusterConnectPool.getConnection()
      it.foreach(x => {
        conn.incrByFloat(x._1,x._2)
      })
    })
  }

  def calculateIncomeByZone(orders: RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {
    val provinceAndMoney = orders.map(order => {
      val ip = order(1)
      val ipLong = IpUtil.ip2Long(ip)
      var province = "未知"
      val rules = broadcastRef.value
      val index: Int = IpUtil.binarySearch(rules, ipLong)
      if (index != -1) {
        province = rules(index)._3
      }
      (province, order(4).toDouble)
    })
    val reduced: RDD[(String, Double)] = provinceAndMoney.reduceByKey(_+_)
    reduced.foreachPartition(it => {
      val conn = JedisClusterConnectPool.getConnection()
      it.foreach(x => {
        conn.incrByFloat(x._1, x._2)
      })
    })
  }
}
