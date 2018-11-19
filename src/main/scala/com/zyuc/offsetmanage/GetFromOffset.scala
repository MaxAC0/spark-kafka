package com.zyuc.offsetmanage

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster

object GetFromOffset {
  /**
    * 获取smallest或者largest的offset
    * @param kafkaParam Kafka configuration parameters
    * @param topics     topic集合, 多个topic使用逗号分隔
    * @return
    */
  def getResetOffsets(kafkaParam: Map[String, String], topics: Set[String]): Map[TopicAndPartition, Long] = {
    val cluster = new KafkaCluster(kafkaParam)
    var offsets: Map[TopicAndPartition, Long] = Map()
    // 最新或者最小offset  reset为smallest或largest
    val reset = kafkaParam.get("auto.offset.reset").map(x => x.toLowerCase())
    val topicAndPartitions: Set[TopicAndPartition] = cluster.getPartitions(topics).right.get
    if (reset.contains("smallest")) {
      val leaderOffsets = cluster.getEarliestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    } else if (reset.contains("largest")) {
      val leaderOffsets = cluster.getLatestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    }
    offsets
  }

  /**
    *
    * @param kafkaParam Kafka configuration parameters
    * @param topicSet topic集合, 多个topic使用逗号分隔
    * @param groupName kafka consumer group
    * @return
    */
  def getConSumerOffsets(zK: OffsetInZK.type , kafkaParam: Map[String, String], topicSet:Set[String], groupName:String) : Map[TopicAndPartition, Long] = {
    val brokers = kafkaParam("metadata.broker.list")
    val kafkaSmallestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val kafkaLargestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    var offsets: Map[TopicAndPartition, Long] = Map()
    val smallOffsets = getResetOffsets(kafkaSmallestParams, topicSet)
    val largestOffsets = getResetOffsets(kafkaLargestParams, topicSet)
    val consumerOffsets = zK.getZKOffsets(topicSet, groupName, kafkaParam) // cOffset-从外部存储中读取的offset
    smallOffsets.foreach({
      case(tp, sOffset) =>
        val cOffset = if (!consumerOffsets.contains(tp)) 0 else  consumerOffsets(tp)
        val lOffset = largestOffsets(tp)
        if(sOffset > cOffset) {
          offsets += tp->sOffset
        } else if(cOffset > lOffset){
          offsets += tp->lOffset
        } else{
          offsets += tp->cOffset
        }

    })
    offsets
  }
}
