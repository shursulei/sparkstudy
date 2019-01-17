package com.shursulei.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
object DataFrameOps2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DataFrameOps")
    conf.setMaster("spark://slq1:7077")
    val sc = new SparkContext
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://slq1:9000/user/data/SparkResources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"),df("age") + 10).show()
    df.filter(df("age") > 10).show()
    df.groupBy("age").count.show()    

  }
}