package com.shursulei.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
/**
  * TopNGroup.scala
  */
object TopNGroup {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Top N Basically!").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("F:\\sparkData\\topNGroup.txt")
    val pairs = lines.map(line => {
      val splited = line.split(" ")
      (splited(0),splited(1).toInt)
    }) //生成Key-Value键值对以方便sortByKey进行排序,Int已经实现了排序比较的接口
    val groupedPairs = pairs.groupByKey() //降序排序
    val sortedPairs = groupedPairs.sortByKey().map(pair =>
      (pair._1,pair._2.toList.sortWith(_ > _).take(5)))
    sortedPairs.collect().foreach(pair => {
      println(pair._1 + " : " + pair._2)
    })
  }
}