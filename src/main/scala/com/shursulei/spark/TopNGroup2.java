package com.shursulei.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 使用java开发topN程序
 *
 */
public class TopNGroup2 {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Top N Group").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//topNGroup.txt");
		// 把每行数据变成符合要求的<Key,Value>格式
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] splitedLine = line.split(" ");
				return new Tuple2<String, Integer>(splitedLine[0], Integer.valueOf(splitedLine[1]));
			}
		});
		JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey(); // 对数据进行分组
		JavaPairRDD<String, Iterable<Integer>> top5 = groupedPairs
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupedData)
							throws Exception {
						// TODO Auto-generated method stub
						Integer[] top5 = new Integer[5]; // 保存top5的数据本身
						String groupedKey = groupedData._1; // 获取分组的组名
						Iterator<Integer> groupedValue = groupedData._2.iterator(); // 获取每组的内容集合
						while (groupedValue.hasNext()) { // 查看是否有下一个元素，如果继续进行循环
							Integer value = groupedValue.next(); // 获取当前循环元素本身的内容
							for (int i = 0; i < 5; i++) { // 具体实现分组内部的topN
								if (top5[i] == null) {
									top5[i] = value;
									break;
								} else if (value > top5[i]) {
									for (int j = 4; j > i; j--) {
										top5[j] = top5[j - 1];
									}
									top5[i] = value;
									break;
								}
							}
						}
						return new Tuple2<String, Iterable<Integer>>(groupedKey, Arrays.asList(top5));
					}
				});

		// 打印分组后的Top N
		top5.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;
			public void call(Tuple2<String, Iterable<Integer>> topped) throws Exception {
				System.out.print("Group key : " + topped._1 + " : "); // 获取Group
				Iterator<Integer> toppedValue = topped._2.iterator(); // 获取Group
																		// Value
				while (toppedValue.hasNext()) { // 具体打印出每组的Top N
					Integer value = toppedValue.next();
					System.out.print(value + " ");
				}
				System.out.println();
			}
		});

		sc.close();
	}

}
