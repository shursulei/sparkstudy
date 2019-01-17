package com.shursulei.spark.sql.byJava;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkSQLJDBC2MySQL {
	public static  void main(String[] args){
	        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLJDBC2MySQL");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        SQLContext sqlContext = new SQLContext(sc);
	        DataFrameReader reader = sqlContext.read().format("jdbc");
	        reader.option("url","jdbc:mysql://Master:3306/spark");//数据库路径
	        reader.option("dbtable","dtspark");//数据表名
	        reader.option("driver","com.mysql.jdbc.Driver");
	        reader.option("user","root");
	        reader.option("password","123456");
	        DataFrame dtsparkDataSourceDFFromMySQL = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
	        reader.option("dbtable","dthadoop");//数据表名
	        DataFrame dthadoopDataSourceDFFromMysql = reader.load();//基于dthadoop表创建DataFrame
	        JavaPairRDD<Tuple2,Integer> resultRDD= dtsparkDataSourceDFFromMySQL.javaRDD().mapToPair(new PairFunction, String, Integer>() {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public <Tuple2,Integer> call(Row row) throws Exception{
	                return new <Tuple2, Integer>(row.getAs("name"),(int) row.getLong(1));
	            }
	        }).join(dthadoopDataSourceDFFromMysql.javaRDD().mapToPair(new PairFunction, String, Integer>() {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public <Tuple2,Integer> call(Row row) throws Exception{
	                return new <Tuple2, Integer>(row.getAs("name"),(int) row.getLong(1));
	            }
	        }));

	        JavaRDD resultRowRDD = resultRDD.map(new Function<Tuple2,Integer>>, Row>() {

	            @Override
	            public Row call(Tuple2, Tuple2, Integer>> tuple) throws Exception {
	                return RowFactory.create(tuple._1,tuple._2._2,tuple._2._1);
	            }
	        });

	        List structFields = new ArrayList();
	        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
	        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
	        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));

	        //构建StructType，用于最后DataFrame元数据的描述
	        StructType structType = DataTypes.createStructType(structFields);
	        DataFrame personsDF = sqlContext.createDataFrame(resultRowRDD,structType);
	        personsDF.show();

	        
	        personsDF.javaRDD().foreachPartition(new VoidFunction>(){
	            @Override
	            public void call(Iterator t) throws Exception{
	                Connection conn2mysql=null;
	                Statement statement =null;
	                try {
	                    conn2mysql = DriverManager.getConnection("jdbc:mysql://master:3306/spark","root","123456");
	                    statement=conn2mysql.createStatement();
	                    while(t.hasNext()){
	                        String sql="insert into nameagescore (name,age,score) values (";
	                        Row row=t.next();
	                        String name=row.getAs("name");
	                        int age= row.getInt(1);
	                        int score =row.getInt(2);
	                        sql+="'"+name+"',"+"'"+age+"',"+"'"+score+"')";
	                        statement.execute(sql);
	                    }
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }finally{
	                    if(conn2mysql !=null){
	                        conn2mysql.close();
	                    }
	                    if(statement !=null){
	                        statement.close();
	                    }
	                }
	            }
	        });
	    }
}
