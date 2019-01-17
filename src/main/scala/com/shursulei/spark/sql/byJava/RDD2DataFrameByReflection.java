package com.shursulei.spark.sql.byJava;
//使用反射的方式将RDD转换成为DataFrame

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RDD2DataFrameByReflection {
	public static void main(String[] args) {
		//创建SparkConf对象
		SparkConf conf= new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
		//创建SparkContext对象
		JavaSparkContext sc= new JavaSparkContext(conf);
		//创建SQLContext上下文对象用于SQL分析
		SQLContext sqlContext=new SQLContext(sc);
		//创建RDD，读取textFile
		JavaRDD<String> lines = sc.textFile("D://scalaeclipse//sparkworkspace//scalaspark//resource//persons.txt");
		JavaRDD<Person> persons= lines.map(new Function<String, Person>(){
		
			@Override
			public Person call(String line) throws Exception {
			String[] splited= line.split(",");
			Person p= new Person();
			p.setId(Integer.valueOf(splited[0].trim()));
			p.setName(splited[1]);
			p.setAge(Integer.valueOf(splited[2].trim()));
			return p;
			}
		});

		/*
		*reateDataFrame方法来自于sqlContext，有两个参数，第一个是RDD，这里就是lines.map之后的persons
		*这个RDD里的类型是person，即每条记录都是person，person其实是有id,name,age的，
		*JavaRDD本身并不知道id,name,age信息，所以要创建DataFrame，DataFrame需要知道id,name,age信息，
		*DataFrame怎么知道的呢？这里用createDataFrame时传入两个参数，第一个的RDD本身，第二个参数是
		*对RDD中每条数据的元数据的描述，这里就是java bean class，即person.class
		*实际上工作原理是：person.class传入时本身会用反射的方式创建DataFrame，
		*在底层通过反射的方式获得Person的所有fields，结合RDD本身，就生成了DataFrame
		*/
		DataFrame df = sqlContext.createDataFrame(persons, Person.class);
		//将DataFrame变成一个TempTable。
		df.registerTempTable("persons");
		//在内存中就会生成一个persons的表，在这张临时表上就可以写SQL语句了。
		DataFrame bigDatas = sqlContext.sql("select * from persons where age >= 6");
		//转过来就可以把查询后的结果变成 RDD。返回的是JavaRDD<Row>
		//注意：这里需要导入org.apache.spark.sql.Row
		JavaRDD<Row> bigDataRDD = bigDatas.javaRDD();
		//再对RDD进行map操作。元素是一行一行的数据(SQL的Row)，结果是Person，再次还原成Person。
		//这里返回的是具体的每条RDD的元素。
		JavaRDD<Person> result= bigDataRDD.map(new Function<Row, Person>(){
			@Override
			public Person call(Row row) throws Exception {
			Person p= new Person();
			//p.setId(row.getInt(0));
			//p.setName(row.getString(1));
			//p.setAge(row.getInt(2));
			//数据读进来时第一列是id，第二列是name,第三列是age,生成的RDD也是这个顺序，
			//变成DataFrame后，DataFrame有自己的优化引擎，优化（数据结构优化等）之后再进行处理，
			//处理后再变成RDD时就不能保证第一列是id，第二列是name,第三列是age了。
			//原因是DataFrame对数据进行了排序。
			p.setId(row.getInt(1));
			p.setName(row.getString(2));
			p.setAge(row.getInt(0));
			return p;
			}
			});
		List<Person> personList = result.collect();
		for(Person p : personList){
			System.out.println(p);
			}
	}
}
