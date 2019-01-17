package com.shursulei.spark.sql.byJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SparkSQLLoadSaveOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByProgrammatically");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame peopleDF = sqlContext.read().format("json")
				.load("D://scalaeclipse//sparkworkspace//scalaspark//resource//people.json");
		peopleDF.select("name").write().mode(SaveMode.Append)
				.save("D://scalaeclipse//sparkworkspace//scalaspark//resource//usersNames");
	}

}
