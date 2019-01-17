package com.shursulei.spark.streaming.byJava;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

public class SparkStreamingPollingDataFromFlume {
	public static void main(String[] args) {
        // TODO Auto-generated method stub
        /**
         * 第一步：配置SparkConf，
         * 1、至少2条线程：因为Spark Streaming应用程序在运行的时候至少有一条
         * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接收的数据（否则的话无法有线程用
         * 于处理数据，随着时间的推移，内存和磁盘都会不堪重负）
         * 2、对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming
         * 应用程序而言，每个Executor一般分配多少Core比较合适？根据经验，5个左右的Core是最佳的
         * （一个段子：分配为奇数个Core表现最佳，例如3个、5个、7个Core等
         */
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingPollingDataFromFlume");
        /**
         * 第二步：创建Spark StreamingContext：
         * 1、这是SparkStreaming应用程序所有功能的起始点和程序调度的核心.
         * SparkStreamingContext的构建可以基于SparkConf参数，也可以基于持久化的SparkStreamingContext的
         * 内容来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
         * 所以需要在Driver重新启动后继续上一次的状态，此时的状态恢复需要基于曾经的Checkpoint）
         * 2、在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用下一个SparkStreaming
         * 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们得出一个重大启发，SparkStreaming框架也只是
         * Spark Core上的一个应用程序而已，只不过Spark Streaming框架想运行的话需要Spark工程师写业务逻辑处理代码
         */
        JavaStreamingContext javassc = new JavaStreamingContext(conf,Durations.seconds(5));
        /**
         * 第三步：创建Spark Streaming输入数据来源input Stream:
         * 1、数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
         * 2、在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据
         *      (当然该端口服务首先必须存在），并且在后续会根据业务需要不断有数据产生（当然对于Spark Streaming
         *      应用程序的运行而言，有无数据其处理流程都是一样的）
         * 3、如果经常在每间隔5秒钟没有数据的话不断启动空的Job其实会造成调度资源的浪费，因为并没有数据需要发生计算；所以
         *      实际的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
         */
        JavaReceiverInputDStream<SparkFlumeEvent> lines = FlumeUtils.createPollingStream(javassc, "Master", 9999);
        /**
         * 第四步：接下来就像对于RDD编程一样基于DStream进行编程，原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
         * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！
         * 
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<SparkFlumeEvent, String>(){ //如果是Scala，由于SAM转换，所以可以写成val words = lines.flatMap(_.split(" ")) 
            public Iterable<String> call(SparkFlumeEvent event) throws Exception {
                String line = event.event().body.array().toString();            
                return Arrays.asList(line.split(" "));
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
                // TODO Auto-generated method stub
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
}
