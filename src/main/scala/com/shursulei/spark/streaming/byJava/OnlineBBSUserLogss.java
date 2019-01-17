package com.shursulei.spark.streaming.byJava;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
/*
 *消费者消费SparkStreamingDataManuallyProducerForKafka类中逻辑级别产生的数据，这里是计算pv，uv，注册人数，跳出率的方式
 */
public class OnlineBBSUserLogss {
	public static void main(String[] args) throws InterruptedException {
		   /**
			 * 第一步：配置SparkConf：
			 * 1，至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
			 * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
			 * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
			 * 2，对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
			 * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
			 * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
			 */
//		  SparkConf conf = new SparkConf().setMaster("spark://h71:7077").setAppName("OnlineBBSUserLogs");
	      SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
	      /**
			 * 第二步：创建SparkStreamingContext：
			 * 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
			 * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容
			 * 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
			 * 所有需要在Driver重新启动后继续上次的状态，此时的状态恢复需要基于曾经的Checkpoint）；
			 * 2，在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用下一个SparkStreamingContext
			 * 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们获得一个重大的启发SparkStreaming框架也只是
			 * Spark Core上的一个应用程序而已，只不过Spark Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
			 */
	      JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
	 
	      /**
			 * 第三步：创建Spark Streaming输入数据来源input Stream：
			 * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
			 * 2, 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
			 * 		的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
			 * 		应用程序的运行而言，有无数据其处理流程都是一样的)； 
			 * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
			 * 		实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
			 */
	      Map<String, String> kafkaParameters = new HashMap<String, String>();
	      kafkaParameters.put("metadata.broker.list","h71:9092,h72:9092,h73:9092");
	      Set topics = new HashSet<String>();
	      topics.add("test");
	      JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
					jsc,
					String.class,
					String.class,
					StringDecoder.class,
					StringDecoder.class,
					kafkaParameters, 
					topics);
	      //在线PV计算
	      onlinePagePV(lines);
	      //在线UV计算
//	      onlineUV(lines);
	      //在线计算注册人数
//	      onlineRegistered(lines);
	      //在线计算跳出率
//	      onlineJumped(lines);
	      //在线不同模块的PV
//	      onlineChannelPV(lines);
	      
	      /*
	       * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
	       * 接受应用程序本身或者Executor中的消息；
	       */
	      jsc.start();
	      jsc.awaitTermination();
	      jsc.close();
	   }
	 
	   private static void onlineChannelPV(JavaPairInputDStream<String, String> lines) {
	      lines.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
	         @Override
	         public Tuple2<String, Long> call(Tuple2<String,String> t) throws Exception {
	            String[] logs = t._2.split("\t");
	            String channelID =logs[4];
	            return new Tuple2<String,Long>(channelID, 1L);
	         }
	      }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
	         @Override
	         public Long call(Long v1, Long v2) throws Exception {
	            return v1 + v2;
	         }
	      }).print();
	   }
	 
	   private static void onlineJumped(JavaPairInputDStream<String, String> lines) {
	      lines.filter(new Function<Tuple2<String,String>, Boolean>() {
	         @Override
	         public Boolean call(Tuple2<String, String> v1) throws Exception {
	            String[] logs = v1._2.split("\t");
	            String action = logs[5];
	            if("View".equals(action)){
	               return true;
	            } else {
	               return false;
	            }
	         }
	      }).mapToPair(new PairFunction<Tuple2<String,String>, Long, Long>() {
	         @Override
	         public Tuple2<Long, Long> call(Tuple2<String,String> t) throws Exception {
	            String[] logs = t._2.split("\t");
	         // Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1"); 这个有错
	            Long usrID = Long.valueOf("null".equals(logs[2])  ? "-1" : logs[2]);
	            return new Tuple2<Long,Long>(usrID, 1L);
	         }
	      }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
	         @Override
	         public Long call(Long v1, Long v2) throws Exception {
	            return v1 + v2;
	         }
	      }).filter(new Function<Tuple2<Long,Long>, Boolean>() {
	         @Override
	         public Boolean call(Tuple2<Long, Long> v1) throws Exception {
	            if(1 == v1._2){
	               return true;
	            } else {
	               return false;
	            }
	         }
	      }).count().print();
	   }
	 
	   private static void onlineRegistered(JavaPairInputDStream<String, String> lines) {
	      lines.filter(new Function<Tuple2<String,String>, Boolean>() {
	         @Override
	         public Boolean call(Tuple2<String, String> v1) throws Exception {
	            String[] logs = v1._2.split("\t");
	            String action = logs[5];
	            if("Register".equals(action)){
	               return true;
	            } else {
	               return false;
	            }
	         }
	      }).count().print();
	   }
	 
	   /**
	    * 因为要计算UV，所以需要获得同样的Page的不同的User，这个时候就需要去重操作，DStreamzhong有distinct吗？当然没有（截止到Spark 1.6.1的时候还没有该Api）
	    * 此时我们就需要求助于DStream魔术般的方法tranform,在该方法内部直接对RDD进行distinct操作，这样就是实现了用户UserID的去重，进而就可以计算出UV了。
	    * @param lines
	    */
	   private static void onlineUV(JavaPairInputDStream<String, String> lines) {
	      /*
	       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
	       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
	       * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	       */
	      JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String,String>, Boolean>() {
	         @Override
	         public Boolean call(Tuple2<String, String> v1) throws Exception {
	            String[] logs = v1._2.split("\t");
	            String action = logs[5];
	            if("View".equals(action)){
	               return true;
	            } else {
	               return false;
	            }
	         }
	      });
	      
	      //在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
	      logsDStream.map(new Function<Tuple2<String,String>,String>(){
	         @Override
	         public String call(Tuple2<String, String> v1) throws Exception {
	            String[] logs =v1._2.split("\t");
	            String usrID = String.valueOf(logs[2] != null ? logs[2] : "-1" );
	            //原文是Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1" );
	            //报错：java.lang.NumberFormatException: For input string: "null"
	            Long pageID = Long.valueOf(logs[3]);
	            return pageID+"_"+usrID;
	         }
	      }).transform(new Function<JavaRDD<String>,JavaRDD<String>>(){
	         @Override
	         public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
	            // TODO Auto-generated method stub
	            return v1.distinct();
	         }
	      }).mapToPair(new PairFunction<String, Long, Long>() {
	         @Override
	         public Tuple2<Long, Long> call(String t) throws Exception {
	            String[] logs = t.split("_");
	            Long pageId = Long.valueOf(logs[0]);
	            return new Tuple2<Long,Long>(pageId, 1L);
	         }
	      }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
	         @Override
	         public Long call(Long v1, Long v2) throws Exception {
	            return v1 + v2;
	         }
	      }).print();
	   }
	 
	   private static void onlinePagePV(JavaPairInputDStream<String, String> lines) {
	      /*
	       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
	       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
	       * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	       */
	      JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String,String>, Boolean>() {
	         @Override
	         public Boolean call(Tuple2<String, String> v1) throws Exception {
	            String[] logs = v1._2.split("\t");
	            String action = logs[5];
	            if("View".equals(action)){
	               return true;
	            } else {
	               return false;
	            }
	         }
	      });
	      
	      //在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
	      JavaPairDStream<Long, Long> pairs = logsDStream.mapToPair(new PairFunction<Tuple2<String,String>, Long, Long>() {
	         @Override
	         public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
	            String[] logs = t._2.split("\t");
	            Long pageId = Long.valueOf(logs[3]);
	            return new Tuple2<Long,Long>(pageId, 1L);
	         }
	      });
	      //在单词实例计数为1基础上，统计每个单词在文件中出现的总次数
	      JavaPairDStream<Long, Long> wordsCount = pairs.reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
	    	//对相同的key，进行Value的累加（包括Local和Reducer级别同时Reduce）
	         @Override
	         public Long call(Long v1, Long v2) throws Exception {
	            return v1 + v2;
	         }
	      });
	      /*
	       * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark Streaming
	       * 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
	       * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
	       * output Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
	       * 方法是foraeachRDD,因为Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
	       * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
	       * 在企業生產環境下，一般會把計算的數據放入Redis或者DB中，采用J2EE等技术进行趋势的绘制等，这就像动态更新的股票交易一下来实现
	       * 在线的监控等；
	       */
	      wordsCount.print();
	   }
}
