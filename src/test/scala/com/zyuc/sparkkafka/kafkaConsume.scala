package com.zyuc.sparkkafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import com.zyuc.spark.utils.utils
import com.zyuc.offsetmanage.OffsetInZK.{getZKOffsets, saveOffsets}

//- kafka

object kafkaConsume {
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.5.125:9081,192.168.5.125:9082"
    val zookeeper = "192.168.5.125:2181"
    val topic = Set[String]("topic_csq", "topic_csq2")
    val groupId = "group_csq"
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka_csq")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> zookeeper)
    kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    var fromOffsets = getZKOffsets(topic, groupId, kafkaParams)
    println("fromOffsets.mkString:", fromOffsets.mkString)
    //fromOffsets: Map[TopicAndPartition, Long] = Map()
    //fromOffsets += TopicAndPartition("topic_csq", 0) -> 25
    //fromOffsets += TopicAndPartition("topic_csq2", 0) -> 0
    println("fromOffsets.mkString:", fromOffsets.mkString)

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    println("========Smallest offsets=============:")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder, (String, String)](
      ssc, kafkaParams, fromOffsets, messageHandler)

    kafkaStream.foreachRDD(r => {
      println("r.partitioner:",r.partitioner,"r.toString:",r.toString())
      val offsetRanges = r.asInstanceOf[HasOffsetRanges].offsetRanges
      println("offsetRanges.mkString:",offsetRanges.mkString)
      if (r.count > 0)
        r.foreach(f => {
          println(f._1, f._2)
        })

      saveOffsets(offsetRanges, groupId)


    })

    println("========Smallest offsets=============:")
    ssc.start()
    ssc.awaitTermination()
  }
}

