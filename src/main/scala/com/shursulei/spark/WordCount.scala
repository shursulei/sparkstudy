package com.shursulei.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD._
/**
  * wordcount的scala编写方式
  */
object WordCount {
  
  def main(args: Array[String]) {
    //设置sparkconf信息
    val conf = new SparkConf()
    conf.setAppName("Wow, My First Spark App!")
    conf.setMaster("local")
    //设置sc
    val sc = new SparkContext(conf)
    //读取数据，选择text方式，hdfs方式，json方式
    val lines = sc.textFile("D://intellIJidea//ScalaProject//resource//helloSpark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = {
      pairs.reduceByKey(_ + _)
    }
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    sc.stop()
  }

}