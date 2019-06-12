package com.wyd.redisutil

import redis.clients.jedis._

object JedisClusterConnectPool {

  private val config: JedisPoolConfig = new JedisPoolConfig;

  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)

  val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
  //Jedis Cluster will attempt to discover cluster nodes automatically
  jedisClusterNodes.add(new HostAndPort("192.168.56.101", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.56.102", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.56.103", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.56.104", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.56.105", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.56.101", 6380))
  //单例模式
  private val jedisCluster = new JedisCluster(jedisClusterNodes)

  def getConnection():JedisCluster = {
    jedisCluster
  }

  def main(args: Array[String]): Unit = {

    val nodes: java.util.Map[String, JedisPool] = jedisCluster.getClusterNodes
    import scala.collection.JavaConversions._
    for(k <- nodes.keySet()){
      val pool: JedisPool = nodes.get(k)
      val conn: Jedis = pool.getResource
      val result: java.util.Set[String] = conn.keys("*")
      for(p <- result) {
        println(p + "->" + jedisCluster.get(p))
        //jedisCluster.del(p)
      }
    }

  }
}
