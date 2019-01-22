package com.shursulei.spark.byJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * SecondarySortKeyApp.java
 */
/**
 * 二次排序，具体实现步骤：
 * 第一步：按照Ordered和Serializable接口实现自定义排序的Key
 * 第二步：将要排序的二次排序的文件加载进<Key, Value>类型的RDD
 * 第三步：使用sortByKey基于自定义的Key进行二次排序
 * 第四步：去除掉排序的Key，只保留排序后的结果
 *
 */
public class SecondarySortKeyApp {
	 /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf conf = new SparkConf().setAppName("SecondarySortKeyApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("F:/helloSpark2.txt",1);
        JavaPairRDD<SecondarySortKey2, String> pairs = line.mapToPair(new PairFunction<String, SecondarySortKey2, String>() {
            public Tuple2<SecondarySortKey2, String> call(String line)
                    throws Exception {
                return new Tuple2<SecondarySortKey2, String>(new SecondarySortKey2(Integer.valueOf(line.split(" ")[0]), Integer.valueOf(line.split(" ")[1])), line);
            }           
        });
        JavaPairRDD<SecondarySortKey2, String> sortedPairs = pairs.sortByKey(false); //完成二次排序
        //过滤掉排序后自定的Key，保留排序的结果
        JavaRDD<String> values = sortedPairs.map(new Function<Tuple2<SecondarySortKey2,String>, String>() {
            public String call(Tuple2<SecondarySortKey2, String> pair)
                    throws Exception {
                return pair._2;
            }
        });
        values.foreach(new VoidFunction<String>() {
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
        sc.close();
    }
}
