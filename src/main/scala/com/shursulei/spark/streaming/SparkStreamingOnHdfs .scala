package com.shursulei.spark.streaming
import org.apache.spark.streaming.dstream.DStream
object SparkStreamingOnHdfs  {
  def main(args: Array[String]) {

    import org.apache.spark._
    import org.apache.spark.streaming._

    /**
      * 第一步：配置Sparkconf：
      * 1、至少有2条线程，因为Spark Streaming应用程序在运行的时候，至少有一条线程用于不断的循环接收数据，并且至少有一条
      * 线程用于处理接收的数据（否则的话，无法有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负。）；
      * 2、对于集群而言，每个Executor一般肯定不止一个线程，对于处理Spark Streaming的应用程序而言，每个Executor一般
      * 分配多少core比较合适？根据经验，5个左右的core是最佳的。
      */
    val conf = new SparkConf()
      .setMaster("spark://cloud001:7077")
      .setAppName("SparkStreamingOnHdfs")
    /**
      * 第二步：创建SparkStreamingContext
      * 1、这个是SparkStreaming一切的开始；
      * 2、状态恢复：可以基于SparkConf参数，也可以基于持久化的SparkStreamingContext进行状态恢复。典型的场景是Driver崩溃后
      * 由于SparkStreaming具有连续不断的24小时不间断的运行，所以需要在Driver重新启动后从上次运行的状态恢复过来，此时的状态需要
      * 基于曾经的CheckPoint。
      */
    val ssc = new StreamingContext(conf, Seconds(15))
    val checkpointDir = "hdfs://cloud001:9000//library/SparkStreaming/CheckPoint_Data"
    ssc.checkpoint(checkpointDir)
    /**
      * 第三步：创建Spark Streaming输入数据来源input stream：
      * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等；
      * 2，在这里我们指定数据来源与网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据，当然，该端口
      * 服务首先必须存在并且在后续会根据业务需要不断的有数据产生；
      * 3，如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实会造成调度资源的浪费，没有数据需要计算，所以企业级生产环境的代码在
      * 具体提交Job前会判断是否有数据，如果没有，就不再提交Job;
      * 4,此处没有Receiver，SparkStreaming应用程序只是按照时间间隔监控 目录下每个Batch新增的内容（把新增）作为RDD的数据来源生成原始RDD
      */
    val lines: DStream[String] = ssc.textFileStream("hdfs://cloud001:9000//library/SparkStreaming/Data")

    /**
      * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！DStream是RDD产生的模板或者说是类。在Spark Streaming发生
      * 计算前，其实质是把每个Batch的DStream的操作翻译成对RDD的操作!
      * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 第4.1步：将每一行的字符串分成单个的单词
      * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word
      */
    val words = lines.flatMap(_.split("\t"))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    /**
      * 此处的print并不会直接触发Job的执行，因为现在的一切都是在Spark Streaming框架控制下，具体要触发，对于Spark Streaming
      * 而言，一般是基于设置的时间间隔Duration。Spark Streaming应用程序要想执行具体的Job，对DSteam就必须有output Stream
      * 操作，output Stream有很多类型的函数触发，例如print，saveAsTextFile,saveAsHadoopFiles等，最为重要的是foreachRDD，
      * 因为处理的结果一般都会放在Redis，DB，DashBoard等，foreachRDD主要就是完成这些功能的，而且可以随意的自定义具体数据放在哪里。
      *
      */
    wordCounts.print()

    /**
      * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的。当然，其内部有消息循环体用于
      * 接收应用程序本身或者Executor中的消息。
      */
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}