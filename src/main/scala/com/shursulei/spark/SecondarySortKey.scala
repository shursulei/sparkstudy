package com.shursulei.spark
import org.apache.spark.{SparkConf, SparkContext}
/**
 * sc.textFile().flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).collect
 * 2 3 
 * 4 1 
 * 3 2 
 * 4 3 
 * 9 7 
 * 2 1
 * 二次排序
 * 所谓二次排序，就是指，排序的时候考虑两个维度
 */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(other: SecondarySortKey): Int = {
    if (this.first - other.first != 0) { this.first - other.first }
    else { this.second - other.second }
  }
}
object SecondarySortKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SecondarySortKey").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//helloSpark2.txt")
    val pairWithSortKey = lines.map(line => {
      (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line)
    }
    )
    val sorted = pairWithSortKey.sortByKey()
    val sortedResult = sorted.map(pair => pair._2)
    sortedResult.collect().foreach(println)
  }
}