package com.zyuc.consumer

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.log4j.Logger

import com.zyuc.offsetmanage.{GetFromOffset, OffsetInZK}
import com.zyuc.offsetmanage.GetFromOffset.getConsumerOffsets

object kafkaConsume {
  val logger = Logger.getLogger("consumer.kafkaConsume")

  def main(args: Array[String]): Unit = {
    val sc = spark.SparkContext.getOrCreate()

    val brokers = sc.getConf.get("spark.app.brokers", "")
    val zookeeper = sc.getConf.get("spark.app.zookeeper", "")
    val topic = sc.getConf.get("spark.app.topic", "").split(",").toSet[String]
    val groupId = sc.getConf.get("spark.app.groupId", "")
    logger.info(s"brokers: $brokers, zookeeper: $zookeeper, topic: $topic, groupId: $groupId")

    //val sparkConf = new SparkConf().setMaster("local[2]").setAppName("mykafka")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // 从zk取断点
    val zk = OffsetInZK.connectString(zookeeper).namespace("mykafka").ExponentialBackoffRetry(1000, 3).offsetRootPath("/consumers/offsets").create()
    // 比较zk断点、largest、smallest
    val fromOffsets = getConsumerOffsets(zk, kafkaParams, topic, groupId)
    logger.info("fromOffsets.mkString:" + fromOffsets.mkString)


    val ssc = new StreamingContext(sc, Seconds(5))
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

    logger.info("========== start loop =============")
    ssc.start()
    ssc.awaitTermination()
  }
}