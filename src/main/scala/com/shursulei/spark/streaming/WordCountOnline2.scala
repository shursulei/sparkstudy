package com.shursulei.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations

object WordCountOnline2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountOnline").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    val lines = ssc.socketTextStream("Master", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val wordsCount = pairs.reduceByKey(_+_)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
}
}