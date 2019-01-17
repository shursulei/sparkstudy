package com.shursulei.spark.sql.byJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
public class DataFrameOps {
	public static void main(String[] args){
		//创建SparkConf用于读取系统配置信息并设置当前应用程序名称
		SparkConf conf=new SparkConf().setAppName("DataFrameOps");
		//创建JavaSparkContext对象实例作为整个Driver核心基石
		JavaSparkContext sc=new JavaSparkContext(conf);
		//创建SQLContext上下文对象用于SQL分析
		SQLContext sqlContext=new SQLContext(sc);
		//创建DataFrame，可以简单认为DataFrame是一张表
		//DataFrame可以来源于多种格式
		//如JSON，可以直接创建DataFrame
		//SQLContext只支持SQL一种方言，用HiveContext就可以支持多种
		//方言（默认HQL，可以设置）。
		DataFrame df=sqlContext.read().json("HDFS://slq1:9000/user/people.json");
		//select * from table
		df.show();
		//describe table;
		df.printSchema();
		df.select("name").show();
		// select name age+10 from table;
		df.select(df.col("name"),df.col("age").plus(10)).show();
		//select * from table where age > 20;
		df.filter(df.col("age").gt(20)).show();
		//select count(1) from table group by age;
		df.groupBy(df.col("age")).count().show();
		}

}
