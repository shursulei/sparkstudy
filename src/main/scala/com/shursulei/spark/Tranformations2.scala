package com.shursulei.spark
import org.apache.spark.{SparkConf, SparkContext}
 
 
object Tranformations2 {
 
 def main(args: Array[String]) {
   val sc = sparkContext("Tranformation Operations") //创建SparkContext
    //  mapTranformation(sc)  //map案例
   //    filterTranformation(sc) //filter案例
    //   flatMapTranformation(sc) //flatMap案例
 
   //    groupByKeyTranformation(sc) //groupByKey案例
 
    //   groupByKeyTranformation(sc) //reduceByKey案例
   // reduceByKeyTranformation(sc)
   joinTranformation(sc) //join案例
 
 
 
 
   sc.stop() //停止SparkContext，销毁相关的Driver对象，释放资源
 }
  def sparkContext( name: String) = {
    val conf = new SparkConf().setAppName("work......").setMaster("local")
    val sc = new SparkContext(conf)
    sc
  }
 
  def mapTranformation(sc: SparkContext){
    val mapData = sc.parallelize( 1 to 10)
    val its = mapData.map(items => items * 2)
    its.collect.foreach(println)
  }
 
  def filterTranformation(sc :SparkContext){
    val filters = sc.parallelize(1 to 20)
    val fd = filters.map(fs => fs % 2 == 0)
    fd.collect.foreach(println)
  }
 
  def flatMapTranformation(sc: SparkContext){
    val flatMap = Array("scala spark", "hadoop scala", "Java tachyon", "Java hadoop")
    val ds = sc.parallelize(flatMap)
    val lds = ds.flatMap(line => line.split(" "))
    lds.collect.foreach(println)
 
  }
 
  def groupByKeyTranformation(sc : SparkContext){
    val data = Array(Tuple2(100, "java"), Tuple2(90, "scala"), Tuple2(80, "oracle"), Tuple2(90, "spark"), Tuple2(100, "oracle"))
    val ss = sc.parallelize(data)
    val rdds = ss.groupByKey()
    rdds.collect.foreach(println)
  }
  def reduceByKeyTranformation(sc: SparkContext){
    val rdds = sc.textFile("D://googledown//datas.txt")
    val words = rdds.flatMap(lines => lines.split(" "))
    val wd = words.map(word => (word, 1))
    val wordOrder = wd.reduceByKey(_+_)
    wordOrder.collect.foreach(pairs => println((pairs._1 +"...."+ pairs._2)))
  }
 
  def joinTranformation(sc: SparkContext){
   /* val  studentName = Array{
      Tuple3(1, "zhangsan");
      Tuple3(2, "lisi", "sh");
      Tuple3(3, "wangwu", "sz");
      Tuple3(4, "wangba", "gz" )
    }
    val studentScore = Array{
      Tuple3(1, 90, "hb");
      Tuple3(2, 80, "zj");
      Tuple3(3, 70, "gd");
      Tuple3(4, 90, "gd")
    }*/
   val studentNames = Array(
     Tuple2(1,"Spark"),
     Tuple2(2,"Tachyon"),
     Tuple2(3,"Hadoop")
   )
 
    val studentScores = Array(
      Tuple2(1,100),
      Tuple2(2,95),
      Tuple2(3,65)
    )
    val names = sc.parallelize(studentNames)
    val score = sc.parallelize(studentScores)
    val joindatas = names.join(score)
    joindatas.collect.foreach(println)
  }
}
