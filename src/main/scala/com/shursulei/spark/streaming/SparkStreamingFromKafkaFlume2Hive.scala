package com.shursulei.spark.streaming
import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * 使用Scala开发集群运行的Spark来实现在线热搜词
  */
case class MessageItem(name: String, age: Int)

object SparkStreamingFromKafkaFlume2Hive {
  def main(args: Array[String]): Unit = {

    if(args.length < 2) {
      System.err.println("Please input your kafka broker list and topics to consume")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SparkStreamingFromKafkaFlume2Hive").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    val Array(brokers, topics) = args
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsParams = topics.split(",").toSet

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsParams)
      .map(_._2.split(",")).foreachRDD(rdd => {
      val hiveContext = new HiveContext(rdd.sparkContext)
      import hiveContext.implicits._
      rdd.map(record => MessageItem(record(0).trim,record(1).trim.toInt)).toDF().registerTempTable("temp")
      hiveContext.sql("SELECT count(*) FROM temp").show()
    })
    // Flume会作为Kafka的Producer把数据写入到Kafka供本程序消费
    ssc.start()
    ssc.awaitTermination()
  }
}