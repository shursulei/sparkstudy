package com.shursulei.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaCluster
import scala.collection.immutable.Map
import java.util.NoSuchElementException
import org.apache.spark.SparkException
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.codehaus.jackson.map.deser.std.PrimitiveArrayDeserializers.StringDeser
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.DirectKafkaInputDStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.HashPartitioner
object SparkStreamingKafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[5]")
     conf.set("spark.streaming.kafka.maxRatePerPartition", "1")
     val sc = new SparkContext(conf)
     val ssc = new StreamingContext(sc,Seconds(1)) 
     ssc.checkpoint("d:\\checkpoint")
     val kafkaParams = Map[String,String](
         "metadata.broker.list"->"bigdata01:9092,bigdata02:9092,bigdata03:9092",
         "group.id"->"group_hgs",
         "zookeeper.connect"->"bigdata01:2181,bigdata02:2181,bigdata03:2181")
     val kc = new KafkaCluster(kafkaParams)
     val topics = Set[String]("test")
     //每个rdd返回的数据是(K,V)类型的，该函数规定了函数返回数据的类型
     val mmdFunct = (mmd: MessageAndMetadata[String, String])=>(mmd.topic+" "+mmd.partition,mmd.message())
      
     val rds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc, kafkaParams, getOffsets(topics,kc,kafkaParams),mmdFunct)
     val updateFunc=(iter:Iterator[(String,Seq[Int],Option[Int])])=>{
        //iter.flatMap(it=>Some(it._2.sum+it._3.getOrElse(0)).map((it._1,_)))//方式一
        //iter.flatMap{case(x,y,z)=>{Some(y.sum+z.getOrElse(0)).map((x,_))}}//方式二
        iter.flatMap(it=>Some(it._1,(it._2.sum.toInt+it._3.getOrElse(0))))//方式三
      }
     val words = rds.flatMap(x=>x._2.split(" ")).map((_,1))
     //val wordscount = words.map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultMinPartitions), true)
     println(getOffsets(topics,kc,kafkaParams))
     rds.foreachRDD(rdd=>{
       if(!rdd.isEmpty()){
         //对每个dataStreamoffset进行更新
         upateOffsets(topics,kc,rdd,kafkaParams)
       }
     }   
    )
     words.print()
     ssc.start()
     ssc.awaitTermination()
}
  def getOffsets(topics : Set[String],kc:KafkaCluster,kafkaParams:Map[String,String]):Map[TopicAndPartition, Long]={
    
    val topicAndPartitionsOrNull =  kc.getPartitions(topics)
    if(topicAndPartitionsOrNull.isLeft){
      throw new SparkException(s"$topics in the set may not found")
    }
    else{
      val topicAndPartitions = topicAndPartitionsOrNull.right.get
      val groups = kafkaParams.get("group.id").get
      val offsetOrNull = kc.getConsumerOffsets(groups, topicAndPartitions)
      if(offsetOrNull.isLeft){
        println(s"$groups you assignment may not exists!now redirect to zero!")
        //如果没有消费过，则从最开始的位置消费
        val erliestOffset = kc.getEarliestLeaderOffsets(topicAndPartitions)
        if(erliestOffset.isLeft)
          throw new SparkException(s"Topics and Partions not definded not found!")
        else
          erliestOffset.right.get.map(x=>(x._1,x._2.offset))
      }
      else{
        //如果消费组已经存在则从记录的地方开始消费
        offsetOrNull.right.get
      }
    }
     
  }
   
  //每次拉取数据后存储offset到ZK
  def upateOffsets(topics : Set[String],kc:KafkaCluster,directRDD:RDD[(String,String)],kafkaParams:Map[String,String]){
    val offsetRanges =  directRDD.asInstanceOf[HasOffsetRanges].offsetRanges
    for(offr <-offsetRanges){
      val topicAndPartitions = TopicAndPartition(offr.topic,offr.partition)
      val yesOrNo = kc.setConsumerOffsets(kafkaParams.get("group.id").get, Map(topicAndPartitions->offr.untilOffset))
      if(yesOrNo.isLeft){
        println(s"Error when update offset of $topicAndPartitions")
      }
    }
  }

/* val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
     val sc = new SparkContext(conf)
     val ssc = new StreamingContext(sc,Seconds(4)) 
    val kafkaParams = Map[String,String](
         "metadata.broker.list"->"bigdata01:9092,bigdata02:9092,bigdata03:9092")
     val kc = new KafkaCluster(kafkaParams)
     
     //获取topic与paritions的信息
     //val tmp = kc.getPartitions(Set[String]("test7"))
     //结果：topicAndPartitons=Set([test7,0], [test7,1], [test7,2])
     //val topicAndPartitons = tmp.right.get
     //println(topicAndPartitons)
     
    //每个分区对应的leader信息
    //val tmp = kc.getPartitions(Set[String]("test7"))
    //val topicAndPartitons = tmp.right.get
    //结果：leadersPerPartitions= Right(Map([test7,0] -> (bigdata03,9092), [test7,1] -> (bigdata01,9092), [test7,2] -> (bigdata02,9092)))
    //val leadersPerPartitions = kc.findLeaders(topicAndPartitons)
    //println(leadersPerPartitions)
     
    //每增加一条消息，对应的partition的offset都会加1,即LeaderOffset(bigdata02,9092,23576)第三个参数会加一
    //val tmp = kc.getPartitions(Set[String]("test"))
    //val topicAndPartitons = tmp.right.get
    //结果t=  Right(Map([test7,0] -> LeaderOffset(bigdata03,9092,23568), [test7,2] -> LeaderOffset(bigdata02,9092,23576), [test7,1] -> LeaderOffset(bigdata01,9092,23571)))
    //val  t = kc.getLatestLeaderOffsets(topicAndPartitons)
    // println(t)
     
     
    //findLeader需要两个参数 topic 分区编号
    //val tmp = kc.findLeader("test7",0)
    //结果leader=RightProjection(Right((bigdata03,9092)))
    //val leader = tmp.right
    //val tp = leader.flatMap(x=>{Either.cond(false, None,(x._1,x._2))})  
     
    val tmp = kc.getPartitions(Set[String]("test"))
    val ttp = tmp.right.get
     
     
    while(true){
      try{
      val tp = kc.getConsumerOffsets("group_test1", ttp)
      val maps = tp.right.get
      println(maps)
      Thread.sleep(2000)
      }
      catch{
        case ex:NoSuchElementException=>{println("test")}
      }
         
    }*/
}