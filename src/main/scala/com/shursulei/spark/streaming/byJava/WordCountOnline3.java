package com.shursulei.spark.streaming.byJava;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.streaming.api.java.JavaPairDStream;
/**
 * 
 * @author shursulei
 *
 */
public class WordCountOnline3 {
	private static final Pattern SPACE=Pattern.compile(" ");
	public static void main(String[] args) {
//		StreamingExamples.setStreamingLogLevels();
		JavaStreamingContext jssc= new JavaStreamingContext("local[2]","JavaNetworkWordCount",new Duration(1000));
		jssc.checkpoint("_");//使用updateStateByKey()函数需要设置checkpoint		
		//打开本地的端口9999
		JavaReceiverInputDStream<String> lines=jssc.socketTextStream("localhost",9999);
		//按行输入，以空格分隔
		JavaDStream<String> words =lines.flatMap(line->Arrays.asList(SPACE.split(line)));
		//每个单词形成pair,如(word,1)
		JavaPairDStream<String,Integer> pairs=words.mapToPair(word->new Tuple2<>(word,1));
		//统计并更新每个单词的历史出现次数
		JavaPairDStream<String,Integer> counts=pairs.updateStateByKey((values,state) -> {
			Integer newSum =state.or(0);
			for(Integer i:values){
				newSum+=i;
			}
			return Optional.of(newSum);
		});
		counts.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
