package com.shursulei.spark.sql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
object RDD2DataFrameByUsingReflection {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setAppName("RDD2DataFrameByProgrammaticallyScala")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val people = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//persons.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")
    val teenagers = sqlContext.sql("select name,age from people where age >=13 and age <=19")
    teenagers.map(t => "Name:" + t(0)).collect().foreach(println)
    teenagers.map(t => "Name:" + t.getAs[String]("name")).collect().foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }
}