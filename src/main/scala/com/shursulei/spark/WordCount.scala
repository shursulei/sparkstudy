package com.shursulei.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object WordCount {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Wow, My First Spark App!")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://intellIJidea//ScalaProject//resource//helloSpark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    sc.stop()
  }

}