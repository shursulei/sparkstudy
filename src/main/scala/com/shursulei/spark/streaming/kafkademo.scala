package com.shursulei.spark.streaming
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object kafkademo {
  def main(args: Array[String]): Unit = {
    //1 创建spark配置对象
    val conf = new SparkConf()
    //设置配置属性
    conf.setMaster("spark://ping1:7077")
    conf.setAppName("test sparkstreaming-kafka consumer")
    //2 通过配置文件创建Stream的上下文对象, 设置Streaming读取每3秒钟流式数据
    val ctx = new StreamingContext(conf, Seconds(3))
    //3 设置kafka读取主题名
    val topics = Array("ping")
    //4 消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> "ping1:9092,ping2:9092,ping4:9092", //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "g1",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean));
    //5 通过 sparkstreaming-kafka的工具类，创建DStream
    //创建DStream，返回接收到的输入数据
    var stream = KafkaUtils.createDirectStream[String, String](ctx, PreferConsistent, Subscribe[String, String](topics, kafkaParam))
    val data = stream.filter(line => line.value().length > 0)
    var rdd = data.map(line => (line.key(), line.value()))
    rdd.saveAsTextFiles("/msparkout")
    // 启动流式读取对象
    ctx.start()
    //等待客户端信息发送
    ctx.awaitTermination()
    //停止流式对象上下文
    ctx.stop()
  }
}