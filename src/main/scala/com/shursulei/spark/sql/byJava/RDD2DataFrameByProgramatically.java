package com.shursulei.spark.sql.byJava;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RDD2DataFrameByProgramatically {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lines = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//persons.txt");
		// 第一步：在RDD的基础上创建类型为Row的RDD
		JavaRDD<Row> personsRDD = lines.map(new Function<String, Row>() {
			@Override
			public Row call(String line) throws Exception {
				String[] splited = line.split(",");
				return RowFactory.create(Integer.valueOf(splited[0]), splited[1], Integer.valueOf(splited[2]));
			}
		});
		// 第二步：动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型，可能来自于JSON文件也可能来自于数据库。JSON非常轻量级，而且天生KV型，非常简洁。数据库则更安全。
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		// 构建SturceType用于最后DataFrame元数据的描述
		StructType structType = DataTypes.createStructType(structFields);
		// 第三步：基于以后的MataData以及RDD<Row>来构造 DataFrame
		// Spark有了DataFrame后会取代RDD？No！RDD是底层的是核心。
		// 今后在编程中是三足鼎立：RDD,DataSet,DataFrame。现在DataSet还是实验性的API
		// DataSet是想让所有的子框架都基于DataSet计算。
		// DataSet底层是钨丝计划。这样的话所有的框架都可以利用Tungsten天然的性能优势。
		// 正常推荐用hiveContext,不是说数据来源是hive才用hiveContext的，hiveContext功能比sqlContext强大，hiveContext包含了sqlContext所有的功能，并在此基础上做的更好。
		DataFrame personsDF = sqlContext.createDataFrame(personsRDD, structType);
		// 第四步：注册成为临时表以供后续的SQL查询操作
		personsDF.registerTempTable("persons");
		// 第五步，进行数据的多维度分析
		DataFrame result = sqlContext.sql("select * from persons where age > 8");
		// 第六步，对结果进行处理，包括由DataFrame转换成为RDD<Row>,以及结果的持久化。
		List<Row> listRow = result.javaRDD().collect();
		for (Row row : listRow) {
			System.out.println(row);
		}
		// 用group/join才能看出Hive和SparkSQL的速度差别。
		// Hadoop和链式操作，一次只能有一个Reducer，有Join/group时会产生很多作业，
		// 而SparkSQL可能只有一个作业
		// Hadoop每一个Task都是一个JVM（JVM不能复用），而Spark是线程复用。
		// JVM生成的时间Spark已经计算完了。
	}
}
