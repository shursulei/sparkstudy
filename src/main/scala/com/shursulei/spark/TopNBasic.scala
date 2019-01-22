package com.shursulei.spark
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD._
/**
 * 注释掉的内容为老师的方法，我改写了适用于RDD的方法
 */
object TopNBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Top N Basically!").setMaster("local")
    val sc = new SparkContext(conf)
    //val lines = sc.textFile("F:\\sparkData\\basicTopN.txt")
    //val pairs = lines.map(line => (line.toInt, line)) //生成Key-Value键值对以方便sortByKey进行排序,Int已经实现了排序比较的接口
    //val sortedPairs = pairs.sortByKey(false) //降序排序
    //val sortedData = sortedPairs.map(_._2)  //过滤出排序后的内容本身
    //val top5 = sortedData.map(_.take(5))  //获取排名前5位的元素内容,元素内容构建成为一个Array
    val lines = sc.textFile("F:\\sparkData\\basicTopN.txt")
    val top5 = lines.map(line => ("tmpKey", line)).groupByKey().flatMap(_._2.toList.sortWith(_ > _).take(5))
    top5.collect().foreach(println)
  }
}