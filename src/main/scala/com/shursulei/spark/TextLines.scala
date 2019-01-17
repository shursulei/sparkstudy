package com.shursulei.spark
import org.apache.spark.{SparkConf, SparkContext}
object TextLines {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到
    conf.setMaster("local") //此时程序在本地运行，不需要安装Spark集群

    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例，来定制Spark运行的具体参数和配置信息
    val lines = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//helloSpark.txt") //通过HadoopRDD以及MapPartitionsRDD获取文件中每一行的内容本身
    val lineCount = lines.map(  (_, 1)) //每一行变成行的内容与1构成的Tuple
    val textLine = lineCount.reduceByKey(_ + _)
    textLine.collect.foreach( pair => println(pair._1 + ":" +pair._2)) //collect是把结果抓到Driver上,foreach的Array中只有一个元素，只不过元素是一个Tuple。


  }

}