package com.zyuc.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object kafkaConsume {
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.5.125:9081,192.168.5.125:9082"
    val zookeeper = "192.168.5.125:2181"
    val topic = Set[String]("topic_csq")
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("kafka")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder ](ssc, kafkaParams, topic)

    println("========Smallest offsets=============:")
    val lines = kafkaStream.map(_._2)
    val words = lines.flatMap(_.split(" ")).map(x=>(x,1))

    words.reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
