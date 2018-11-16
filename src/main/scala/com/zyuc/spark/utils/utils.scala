package com.zyuc.spark.utils

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils, OffsetRange}
import scala.collection.JavaConversions._

import com.zyuc.offsetmanage.OffsetInZK.getZKOffsets

/**
  * Created by zhoucw on 上午2:18.
  */
object utils {





  /**
    *
    * @param kafkaParam
    * @param topicSet
    * @param groupName
    * @return
    */
  def getConSumerOffsets(kafkaParam: Map[String, String], topicSet:Set[String], groupName:String) : Map[TopicAndPartition, Long] = {
    val brokers = kafkaParam("metadata.broker.list")
    val kafkaSmallestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val kafkaLargestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    var offsets: Map[TopicAndPartition, Long] = Map()
    val smallOffsets = getResetOffsets(kafkaSmallestParams, topicSet)
    val largestOffsets = getResetOffsets(kafkaLargestParams, topicSet)
    val consumerOffsets = getZKOffsets(topicSet, groupName, kafkaParam) // cOffset-从外部存储中读取的offset
    smallOffsets.foreach({
      case(tp, sOffset) => {
        val cOffset = if (!consumerOffsets.containsKey(tp)) 0 else  consumerOffsets(tp)
        val lOffset = largestOffsets(tp)
        if(sOffset > cOffset) {
          offsets += tp->sOffset
        } else if(cOffset > lOffset){
          offsets += tp->lOffset
        } else{
          offsets += tp->cOffset
        }
      }
    })
    offsets
  }

  /**
    * 获取smallest或者largest的offset
    * @param kafkaParam
    * @param topics     topic集合, 多个topic使用逗号分隔
    * @return
    */
  def getResetOffsets(kafkaParam: Map[String, String], topics: Set[String]): Map[TopicAndPartition, Long] = {
    val cluster = new KafkaCluster(kafkaParam)
    var offsets: Map[TopicAndPartition, Long] = Map()
    // 最新或者最小offset  reset为smallest或largest
    val reset = kafkaParam.get("auto.offset.reset").map(x => x.toLowerCase())
    val topicAndPartitions: Set[TopicAndPartition] = cluster.getPartitions(topics).right.get
    if (reset == Some("smallest")) {
      val leaderOffsets = cluster.getEarliestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    } else if (reset == Some("largest")) {
      val leaderOffsets = cluster.getLatestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    }
    offsets
  }

  def createMyDirectKafkaStream (ssc: StreamingContext,kafkaParams: Map[String, String], topics: Set[String], groupName: String
                                ): InputDStream[(String, String)] = {
    val fromOffsets = getConSumerOffsets(kafkaParams, topics, groupName)
    println("fromOffsets==" + fromOffsets)
    var kafkaStream : InputDStream[(String, String)] = null
    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    kafkaStream
  }

  def main(args: Array[String]): Unit = {
    val brokers = "192.168.5.125:9081"
    val topic = "topic_csq" //
    val topicsSet = topic.split(",").toSet
    // 获取topic中有效的最小offset
    val kafkaParamsSmallest = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val smallestOffsets = getResetOffsets(kafkaParamsSmallest, topicsSet)
    // 获取topic中有效的最新offset
    //val kafkaParamsLargest = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    //val largestOffsets = getResetOffsets(kafkaParamsLargest, topicsSet)
    // 打印
    println("========Smallest offsets=============:" + smallestOffsets)
    //println("========Largest offsets=============:" + largestOffsets)
    //println(getZKOffsets(Set("dd,mytest1"), "abc"))
  }
}
