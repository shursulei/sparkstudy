package com.shursulei.spark.sql.byJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import java.util.List;
public class SparkSQLParquetOps2 {
	 public static void main(String[] args){
	        //创建SparkConf用于读取系统信息并设置运用程序的名称
	        SparkConf conf = new SparkConf().setAppName("DataFrameOps").setMaster("local");
	        //创建JavaSparkContext对象实例作为整个Driver的核心基石
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        //设置输出log的等级
	        sc.setLogLevel("INFO");
	        //创建SQLContext上下文对象，用于SqL的分析
	        SQLContext sqlContext = new SQLContext(sc);
//	        Dataset userDS =sqlContext.read().parquet("/usr/local/spark/examples/src/main/resources/users.parquet");
            DataFrame userDS =sqlContext.read().parquet("/usr/local/spark/examples/src/main/resources/users.parquet");
	        ((DataFrame) userDS).registerTempTable("users");
//	        Dataset result = sqlContext.sql("select name from users");
	        DataFrame result = sqlContext.sql("select name from users");
	        JavaRDD<String> resultRDD = result.javaRDD().map(new Function<Row, String>() {
	            @Override
	            public String call(Row row) throws Exception {
	                return "This name is :"+row.getAs("name");
	            }
	        });
	        List<String> listRow = resultRDD.collect();
	        for (String row:listRow){
	            System.out.println(row);
	        }
	    }
}
