package com.shursulei.spark.sql
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD._
/**
 * 复杂Top N案例实战
 */
object TopNGroup {
  def main(args: Array[String]) {
    /**
     *     * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
     *     * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
     *     * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
     *     * 只有1G的内存）的初学者       *
     */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Top N Basically!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local")
    /**
     *     * 第2步：创建SparkContext对象
     *     * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
     *     * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
     *     * 同时还会负责Spark程序往Master注册程序等
     *     * SparkContext是整个Spark应用程序中最为至关重要的一个对象
     *     
     */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    sc.setLogLevel("OFF")
    /**
     *     * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
     *     * RDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作
     *     * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
     *     
     */
    val lines = sc.textFile("D://DT-IMF//testdata//topNGroup.txt") //读取本地文件并设置为一个Partition
    val groupRDD = lines.map(line => (line.split(" ")(0), line.split(" ")(1).toInt)).groupByKey()
    val top5 = groupRDD.map(pair => (pair._1, pair._2.toList.sortWith(_ > _).take(5))).sortByKey()
    top5.collect().foreach(pair => {
      println(pair._1 + ":")
      pair._2.foreach(println)
      println("*********************")
    })
  }

}