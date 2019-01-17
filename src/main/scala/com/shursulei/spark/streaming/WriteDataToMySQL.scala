package com.shursulei.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.shursulei.spark.streaming.byJava.ConnectPool

object WriteDataToMySQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WriteDataToMySQL")
    val ssc = new StreamingContext(conf,Seconds(5))
    // 假设socket输入的数据格式为：searchKeyword,time
    val ItemsStream = ssc.socketTextStream("spark-master",9999)
    // 将输入数据变成(searchKeyword,1)
    var ItemPairs = ItemsStream.map(line =>(line.split(",")(0),1))
     val ItemCount = ItemPairs.reduceByKeyAndWindow((v1:Int,v2:Int)=> v1+v2,Seconds(60),Seconds(10))
    //ssc.checkpoint("/user/checkpoints/")
    // val ItemCount = ItemPairs.reduceByKeyAndWindow((v1:Int,v2:Int)=> v1+v2,(v1:Int,v2:Int)=> v1-v2,Seconds(60),Seconds(10))
    /**
     * 接下来需要对热词的频率进行排序，而DStream没有提供sort的方法。那么我们可以实现transform函数，用RDD的sortByKey实现
     */
    val hottestWord = ItemCount.transform(itemRDD => {
      val top3 = itemRDD.map(pair => (pair._2, pair._1))
        .sortByKey(false).map(pair => (pair._2, pair._1)).take(3)
      ssc.sparkContext.makeRDD(top3)
    })
    hottestWord.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords =>{
        val conn = ConnectPool.getConnection
        conn.setAutoCommit(false);  //设为手动提交
        val  stmt = conn.createStatement();
        partitionOfRecords.foreach( record => {
          stmt.addBatch("insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'"+record._1+"','"+record._2+"')");
        })
        stmt.executeBatch();
        conn.commit();  //提交事务
      })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}