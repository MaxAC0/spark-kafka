package com.zyuc.test

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import com.zyuc.offsetmanage.{GetFromOffset, OffsetInZK}
import com.zyuc.offsetmanage.GetFromOffset.getConsumerOffsets
import kafka.common.TopicAndPartition
import org.apache.log4j.Logger


object kafkaConsume {
  val logger = Logger.getLogger("consumer")
  def main(args: Array[String]): Unit = {
    //val sc = spark.SparkContext.getOrCreate().

    //var brokers = sc.getConf.get("spark.app.brokers", "")
    val brokers = "192.168.5.125:9081,192.168.5.125:9082"
    //var zookeeper = sc.getConf.get("spark.app.zookeeper", "")
    val zookeeper = "192.168.5.125:2181"
    //var topic = sc.getConf.get("spark.app.topic", "").split(",").toSet[String]
    val topic = Set[String]("topic_csq", "topic_csq2")
    //var groupId = sc.getConf.get("spark.app.groupid", "")
    val groupId = "group_csq"

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("mykafka")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // 从zk取断点
    val zk = OffsetInZK.connectString(zookeeper).namespace("mykafka").ExponentialBackoffRetry(1000, 3).offsetRootPath("/consumers/offsets").create()
    // 比较zk断点、largest、smallest
    var fromOffsets = getConsumerOffsets(zk, kafkaParams, topic, groupId)
    logger.info("fromOffsets.mkString:" + fromOffsets.mkString)
    //fromOffsets: Map[TopicAndPartition, Long] = Map()
    //fromOffsets += TopicAndPartition("topic_csq", 0) -> 25
    //fromOffsets += TopicAndPartition("topic_csq2", 0) -> 0
    println("fromOffsets.mkString:", fromOffsets.mkString)

    // 单独取zk断点
    var offset = zk.getZKOffsets(topic, groupId, kafkaParams)
    logger.info("zk记录的断点 offset.mkString:" + offset.mkString)
    // smallest
    var params = scala.collection.mutable.Map() ++ kafkaParams //可变map
    params += ("auto.offset.reset" -> "smallest")
    offset = GetFromOffset.getResetOffsets(Map[String, String]() ++ params, topic)
    println("kakfa smallest:", offset.mkString)
    // largest
    params("auto.offset.reset") = "largest"
    offset = GetFromOffset.getResetOffsets(Map[String, String]() ++ params, topic)
    println("kakfa largest:", offset.mkString)

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    // 从断点fromOffsets开始读数据
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder, (String, String)](
      ssc, kafkaParams, fromOffsets, messageHandler)

    // 处理逻辑
    kafkaStream.foreachRDD(r => {
      println("r.partitioner:",r.partitioner,"r.toString:",r.toString())
      val offsetRanges = r.asInstanceOf[HasOffsetRanges].offsetRanges
      println("offsetRanges.mkString:",offsetRanges.mkString)
      if (r.count > 0)
        r.foreach(f => {
          println(f._1, f._2)
        })

      zk.saveOffsets(offsetRanges, groupId)
    })

    println("========== start loop =============")
    ssc.start()
    ssc.awaitTermination()
  }
}