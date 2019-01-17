package com.shursulei.spark.streaming.byJava;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class SparkStreamingOnKafkaReceiver {
	public static void main(String[] args) {
        // TODO Auto-generated method stub
        //好处：1、checkpoint 2、工厂
        final SparkConf conf = new SparkConf().setAppName("SparkStreamingOnKafkaReceiver").setMaster("hdfs://Master:7077/");
        final String checkpointDirectory = "hdfs://Master:9000/library/SparkStreaming/CheckPoint_Data";
        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext(checkpointDirectory, conf);
            }
        };
        /**
         * 可以从失败中恢复Driver，不过还需要指定Driver这个进程运行在Cluster，并且在提交应用程序的时候制定--supervise;
         */
        JavaStreamingContext javassc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
        /**
         * 第三步：创建Spark Streaming输入数据来源input Stream:
         * 1、数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
         * 2、在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据
         *      (当然该端口服务首先必须存在），并且在后续会根据业务需要不断有数据产生（当然对于Spark Streaming
         *      应用程序的运行而言，有无数据其处理流程都是一样的）
         * 3、如果经常在每间隔5秒钟没有数据的话不断启动空的Job其实会造成调度资源的浪费，因为并没有数据需要发生计算；所以
         *      实际的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
         */

        //第一个参数是StreamingContext实例
        //第二个参数是ZooKeeper集群信息（接收Kafka数据的时候从ZooKeeper中获得Offset等元数据信息）
        //第三个参数是Consumer Group
        //第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
        Map<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();
        topicConsumerConcurrency.put("HelloKafkaFromSparkStreaming", 2); 
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(javassc,
                "Master:2181,Worker1:2181,Worker2:2181", 
                "MyFirstConsumerGroup", 
                topicConsumerConcurrency);
        /**
         * 第四步：接下来就像对于RDD编程一样基于DStream进行编程，原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
         * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！
         * 
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>(){ //如果是Scala，由于SAM转换，所以可以写成val words = lines.flatMap(_.split(" ")) 
            public Iterable<String> call(Tuple2<String, String> tuple) {
                return Arrays.asList(tuple._2.split(" "));
            }
        });
        /**
         * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
         */
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer> (word, 1);
            }       
        });
     /**
     * 第4.3步：在单词实例计数为1基础上，统计每个单词在文件中出现的总次数
     */
        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
            //对相同的key，进行Value的累加（包括Local和Reducer级别同时Reduce）
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        /**
         * 此处的print并不会直接触发Job的支持，因为现在的一切都是在Spark Streaming框架的控制之下的，对于SparkStreaming
         * 而言，具体是否触发真正的Job运行是基于设置的Duration时间间隔的
         * 
         * 注意，Spark Streaming应用程序要想执行具体的Job，对DStream就必须有ouptputstream操作
         * outputstream有很多类型的函数触发，例如print,saveAsTextFile,saveAsHadoopFiles等，
         * 其中最为重要的一个方法是foreachRDD，因为Spark Streaming处理的结果一般会放在Redis、DB、DashBoard
         * 等上面，所以foreachRDD主要就是用来完成这些功能的，而且可以随意自定义具体数据到底放在哪里。
         */
        wordsCount.print();
        /**
         * Spark Streaming 执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
         * 接收应用程序本身或者Executor中的消息，
         */
        javassc.start();
        javassc.awaitTermination();
        javassc.close();
    }
    private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {
        // If you do not see this printed, that means the StreamingContext has been loaded
        // from the new checkpoint
        System.out.println("Creating new context");
        // Create the context with a 5 second batch size
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
        ssc.checkpoint(checkpointDirectory);
        return ssc;
    }
}
