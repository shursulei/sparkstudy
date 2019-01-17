package com.shursulei.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.shursulei.spark.streaming.byJava.ConnectionPool
/**
 * 把处理后的数据放到外部存储介质上，本次案例是mysql数据库
 */
object OnlineForeachRDD2DB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("OnlineForeachRDD2DB").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("Master", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords => {
        val connection = ConnectionPool.getConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into streaming_itemcount(item,count) values('" + record._1 + "'," + record._2 + ")"
          val stmt = connection.createStatement
          stmt.executeUpdate(sql)
        })
        ConnectionPool.returnConnection(connection)
      }
      }
    }
  }
}