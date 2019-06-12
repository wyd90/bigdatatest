package com.wyd.kafka080spark

import com.wyd.redisutil.JedisClusterConnectPool
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.JedisCluster

object Kafka080DirectWordCount {

  def main(args: Array[String]): Unit = {
    //指定组名
    val group = "ndg1"
    //创建SparkConf
    val conf = new SparkConf().setAppName("Kafka080DirectWordCount").setMaster("local[*]")
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))
    //指定消费的topic名字
    val topic = "nwordcount"
    //指定Kafka的broker地址
    val brokerList = "node2:9092,node3:9092,node4:9092"
    //指定zk的地址，后期更新消费的偏移量时使用（以后可以使用Redis、MySQL来记录偏移量）
    val zkQuorum = "node3:2181,node4:2181,node5:2181"
    //创建stream时使用的topic名字集合，SparkStreaming可以同时消费多个topic
    val topics: Set[String] = Set(topic)
    //创建一个ZKGroupTopicDirs对象
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)
    //获取zookeeper中的路径 "/ndg1/offsets/nwordcount"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    //smallest代表最开始
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    //创建zookeeper客户端，用户从zk中读取偏移量，并更新偏移量
    val zkClient: ZkClient = new ZkClient(zkQuorum)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同partition时生成）
    //  组  /       / topic   /分区/ 偏移量
    // /a001/offsets/nwordcount/0/10001
    // /a001/offsets/nwordcount/1/61201
    // /a001/offsets/nwordcount/2/30001
    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String,String)] = null
    //如果zookeeper中保存offset，我们会利用这个offset作为kafkaStream的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if(children > 0) {
      for(i <- 0 until children){
        val partitionOffset: String = zkClient.readData[String](s"${zkTopicPath}/${i}")
        val tp: TopicAndPartition = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //Key:kafka的key  values：内容"hello tom hell jerry"
      //这个会将kafka的消息进行transform，最终kafka的数据会变成（kafka的key，message）这样的tuple
      val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.key(),mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String,String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      //从头读                                        kafka中的key，v   key的解析器      v的解析器
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    }

    var offsetRanges = Array[OffsetRange]()
    //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能调用DStream的Transformation
    //所以只能在KafkaStream中调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
    //依次迭代KafkaDStream中的KafkaRDD
    //KafkaStream.foreachRDD里面的业务逻辑是在Driver端执行的
    kafkaStream.foreachRDD(kafkaRDD => {
      if(!kafkaRDD.isEmpty()){
        //只有kafkaRDD实现了HasOffsetRanges特质
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        //.map()是在executer中执行
        val message: RDD[String] = kafkaRDD.map(_._2)

        val result: RDD[(String, Int)] = message.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


        result.foreachPartition(it => {
          val conn: JedisCluster = JedisClusterConnectPool.getConnection()
          it.foreach(x => {
            println(x._1 + ":"+x._2)
            conn.incrBy(x._1,x._2.toLong)
          })
        })

        for(o <- offsetRanges){
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"

          ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        }
      }


    })

    ssc.start()
    ssc.awaitTermination()

  }

}
