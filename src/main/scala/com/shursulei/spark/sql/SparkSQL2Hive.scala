package com.shursulei.spark.sql
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
object SparkSQL2Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQL2Hive")
    val sc = new SparkContext(conf)

    /**
      * 第一：在目前企业级大数据Spark开发的时候绝大多数情况下是采用Hive作为数据仓库的；
      * Spark提供了Hive的支持功能，Spark通过HiveContext可以直接操作Hive中的数据；
      * 基于HiveContext我们可以使用SQL/HiveQL两种方式来编写SQL语句对Hive进行操作，
      * 包括创建表、删除表、向表里导入数据以及用SQL语法构造各种SQL语句对表中的数据进行Crud操作
      * 第二：我们也可以直接通过saveAsTable的方式把DataFrame中的数据保存到Hive数据仓库中
      * 第三：可以直接通过HiveContext.table方法来直接加载Hive中的表而生成DataFrame
      */
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("use hive") //使用Hive数据仓库中的hive数据库
    hiveContext.sql("DROP TABLE IF EXISTS people")  //删除同名的表
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")  //创建自定义的表
    /**
      * 把本地数据加载到Hive数据仓库中（背后实际上发生了数据的拷贝）
      * 当然也可以通过LOAD DATA INPATH去获得HDFS等上面的数据到Hive（此时发生了数据的移动）
      */
    hiveContext.sql("LOAD DATA LOCAL INPATH 'F:/sparkData/people.txt' INTO TABLE people")

    hiveContext.sql("DROP TABLE IF EXISTS peoplescores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH 'F:/sparkData/peoplescores.txt' INTO TABLE people")

    /**
      * 通过HiveContext使用join直接基于Hive中的两张表进行操作获得大于90分的人的name, age, score
      */
    val resultDF = hiveContext.sql("SELECT  pi.name,pi.age,ps.score  FROM people pi JOIN peoplescores ps " +
      "ON pi.name=ps.name WHERE ps.score>90")

    /**
      * 通过saveAsTable创建一张Hive Managed Table，数据的元数据和数据即将放的具体位置都是
      * 由Hive数据仓库进行管理的，当删除该表的时候，数据也会一起被删除（磁盘上的数据不再存在）
      */
    hiveContext.sql("DROP TABLE IF EXISTS peopleinformationresult")
    resultDF.saveAsTable("peopleinformationresult")

    /**
      * 使用HiveContext的table可以直接去读Hive数据仓库中的Table并生成DataFrame，
      * 接下来就可以进行机器学习、图计算、各种复杂ETL等操作
      */
    val dataFromHive = hiveContext.table("peopleinformationresult")
    dataFromHive.show()



  }

}