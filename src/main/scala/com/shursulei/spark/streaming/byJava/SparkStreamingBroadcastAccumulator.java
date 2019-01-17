package com.shursulei.spark.streaming.byJava;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.parse.HiveParser.ifExists_return;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
public class SparkStreamingBroadcastAccumulator {
	private static volatile Broadcast<List<String>> broadcastList = null;
    private static volatile Accumulator<Integer> accumulator = null;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //好处：1、checkpoint 2、工厂
        SparkConf conf = new SparkConf().setAppName("SparkStreamingBroadcastAccumulator").setMaster("hdfs://Master:7077/");

        JavaStreamingContext javassc = new JavaStreamingContext(conf, Durations.seconds(15));
        //没有action广播不会发出
        //使用Broadcast广播黑名单到每个Executor中
        broadcastList = javassc.sparkContext().broadcast(Arrays.asList("Hadoop","Mahout","Hive"));
        //全局计数器，用于统计在线过滤了多少个黑名单
        accumulator = javassc.sparkContext().accumulator(0, "OnlineBlacklistCounter");
        //创建Kafka元数据来让Spark Streaming这个Kafka Consumer利用

        JavaReceiverInputDStream<String> lines = javassc.socketTextStream("Master", 9999);


        JavaPairDStream<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String t) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<String, Integer>(t, 1);
            }
        });

        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
            //对相同的key，进行Value的累加（包括Local和Reducer级别同时Reduce）
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1 + v2;
            }           
        });

        wordsCount.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

            public Void call(JavaPairRDD<String, Integer> rdd, Time time)
                    throws Exception {
                // TODO Auto-generated method stub
                rdd.filter(new Function<Tuple2<String,Integer>, Boolean>() {    
                    public Boolean call(Tuple2<String, Integer> wordPair) throws Exception {
                        if(broadcastList.value().contains(wordPair._1)) {
                            accumulator.add(wordPair._2);
                            return false;
                        } else {
                            return true;
                        }
                    }
                }).collect();
                System.out.println(broadcastList.value().toString() + " : " + accumulator.value());
                return null;
            }           
        });
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
