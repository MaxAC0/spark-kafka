package com.zyuc.offsetmanage

import org.apache.log4j.Logger
import kafka.common.TopicAndPartition
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.kafka.OffsetRange

object OffsetInZK {
  val logger = Logger.getLogger("offsetmanage.OffsetInZK")

  private var zkClient: CuratorFramework = _
  // connect
  private var namespace: String = ""
  private var connectStr: String = ""
  private var baseSleepTimeMs: Int = 1000
  private var maxRetries: Int = 3

  // config
  private var offsetRootPath = "/consumers/offsets"

  def connectString(connectStr: String) : this.type = {
    this.connectStr = connectStr
    this
  }

  // 应用隔离.在zookeeper中,kafka的offset保存的根目录
  def namespace(namespace: String) : this.type = {
    this.namespace = namespace
    this
  }

  def ExponentialBackoffRetry(baseSleepTimeMs: Int, maxRetries: Int) : this.type = {
    this.baseSleepTimeMs = baseSleepTimeMs
    this.maxRetries =  maxRetries
    this
  }

  def offsetRootPath(offsetRootPath: String) : this.type = {
    this.offsetRootPath =  offsetRootPath
    this
  }

  def create(): this.type = {
    val client = CuratorFrameworkFactory.builder.connectString(this.connectStr).
      retryPolicy(new ExponentialBackoffRetry(1000, 3)).namespace(this.namespace).build()
    client.start()

    this.zkClient = client
    logger.info(s"connected to zk with parameters(connectString: ${this.connectStr},namespace: ${this.namespace}, retryPolicy: ExponentialBackoffRetry(1000, 3))")
    this
  }

  /**
    * 判断zookeeper的路径是否存在, 如果不存在则创建
    * @param path  zookeeper的目录路径
    */
  def ensureZKPathExists(path: String): Unit = {
    val zkClient = this.zkClient
    if (zkClient.checkExists().forPath(path) == null) {
      logger.info(s"path not exists in zk, create now")
      zkClient.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  /**
    * 将断点按group+topic+partition记录到zk
    * @param offsetsRanges  [Map(topic: String, partition: Int, fromOffset: Long, untilOffset: Long)]
    * @param groupName kafka consumer group
    */
  def saveOffsets (offsetsRanges:Array[OffsetRange], groupName:String) : Unit = {
    var kakfaOffsetRootPath = this.offsetRootPath
    val zkClient = this.zkClient
    for (o <- offsetsRanges) {
      val zkPath = s"$kakfaOffsetRootPath/$groupName/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)

      logger.info(s"set offset of group($groupName),topic(${o.topic}),partition(${o.partition}) to zkPath, value is ${o.untilOffset.toString}")
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
    val kakfaOffsetRootPath = this.offsetRootPath
    val zkClient = this.zkClient
    var offsets: Map[TopicAndPartition, Long] = Map()
    // get /$kakfaOffsetRootPath/$groupname/$topic/$partition
    val offGroupPath = kakfaOffsetRootPath + "/" + groupName
    // 如果路径不存在， 则offset没有保存
    if (zkClient.checkExists().forPath(offGroupPath) == null) {
      logger.info(s"offGroupPath +  not exists!")
      return offsets
    }

    import scala.collection.JavaConversions._
    for {
      topic<-zkClient.getChildren.forPath(offGroupPath)
      if topicSet.contains(topic)
      partition <- zkClient.getChildren.forPath(offGroupPath + "/" + topic)
    } yield {
      val partionPath = offGroupPath + "/" + topic + "/" + partition
      val offset =  zkClient.getData.forPath(partionPath) // if (zkClient.checkExists().forPath(partionPath) != null) zkClient.getData.forPath(partionPath) else "-1"
      offsets += TopicAndPartition(topic, Integer.parseInt(partition)) -> java.lang.Long.valueOf(new String(offset)).toLong

      logger.info(s"groupName: $groupName, topic: $topic, partition: $partition, offset in zk: ${java.lang.Long.valueOf(new String(offset))}")
    }
    offsets
  }
}
