package com.shursulei.spark.byJava;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class Cogroups {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("cogroups").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, String>> nameList = Arrays.asList(new Tuple2<Integer, String>(1, "java"),
				new Tuple2<Integer, String>(2, "spark"), new Tuple2<Integer, String>(3, "hadoop"));
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90), new Tuple2<Integer, Integer>(3, 70),
				new Tuple2<Integer, Integer>(1, 60), new Tuple2<Integer, Integer>(2, 80),
				new Tuple2<Integer, Integer>(2, 50));
		JavaPairRDD<Integer, String> name = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> score = sc.parallelizePairs(scoreList);
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = name.cogroup(score);
		cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> datas) throws Exception {
				System.out.println("id:" + datas._1);
				System.out.println("name:" + datas._2._1);
				System.out.println("score" + datas._2._2);
				System.out.println(".................................");
			}
		});
		sc.close();
	}
}
