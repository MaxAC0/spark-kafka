package com.zyuc.offsetmanage

import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.kafka.OffsetRange
import scala.collection.JavaConversions._

import com.zyuc.spark.utils.utils.getResetOffsets

object OffsetInZK {
  // 在zookeeper中,kafka的offset保存的根目录
  val namespace = "mykafka"
  //  kafka的offset保存的工作目录,/$kakfaOffsetRootPath/$groupname/$topic/$partition
  val kakfaOffsetRootPath = "/consumers/offsets"
  // 初始化Zookeeper客户端
  val zkClient = {
    val client = CuratorFrameworkFactory.builder.connectString("192.168.5.125:2181").
      retryPolicy(new ExponentialBackoffRetry(1000, 3)).namespace(namespace).build()
    client.start()
    client
  }

  /**
    * 判断zookeeper的路径是否存在, 如果不存在则创建
    * @param path  zookeeper的目录路径
    */
  def ensureZKPathExists(path: String): Unit = {
    if (zkClient.checkExists().forPath(path) == null) {
      zkClient.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  /**
    * 将断点按group+topic+partition记录到zk
    * @param offsetsRanges  [Map(topic: String, partition: Int, fromOffset: Long, untilOffset: Long)]
    * @param groupName kafka consumer group
    */
  def saveOffsets (offsetsRanges:Array[OffsetRange], groupName:String) = {
    for (o <- offsetsRanges) {
      val zkPath = s"$kakfaOffsetRootPath/${groupName}/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)
      zkClient.setData().forPath(zkPath,o.untilOffset.toString.getBytes())
    }
  }

  /**
    * 从zk取出保存的断点
    * @param topicSet  [Map(topic: String, partition: Int, fromOffset: Long, untilOffset: Long)]
    * @param groupName kafka consumer group
    * @param kafkaParam Kafka configuration parameters
    *
    */
  def getZKOffsets(topicSet:Set[String], groupName:String, kafkaParam: Map[String, String]) : Map[TopicAndPartition, Long] = {
    var offsets: Map[TopicAndPartition, Long] = Map()
    // get /$kakfaOffsetRootPath/$groupname/$topic/$partition
    val offGroupPath = kakfaOffsetRootPath + "/" + groupName
    // 如果路径不存在， 则offset没有保存
    if (zkClient.checkExists().forPath(offGroupPath) == null) {
      println(offGroupPath + " not exists!")
      return offsets
    }
    offsets = getResetOffsets(kafkaParam, topicSet )
    for{
      topic<-zkClient.getChildren.forPath(offGroupPath)
      if topicSet.contains(topic)
      partition <- zkClient.getChildren.forPath(offGroupPath + "/" + topic)
    } yield {
      val partionPath = offGroupPath + "/" + topic + "/" + partition
      val offset =  zkClient.getData.forPath(partionPath) // if (zkClient.checkExists().forPath(partionPath) != null) zkClient.getData.forPath(partionPath) else "-1"
      offsets += TopicAndPartition(topic, Integer.parseInt(partition)) -> java.lang.Long.valueOf(new String(offset)).toLong
    }
    offsets
  }
}
