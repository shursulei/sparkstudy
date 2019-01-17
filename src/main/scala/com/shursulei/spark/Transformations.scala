package com.shursulei.spark
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 最常用、最重要的SparkTransformation案例实战
  */
object Transformations {
  def main(args: Array[String]) {
    val sc = sparkContext("Transformation") //创建SparkContext
    mapTransformation(sc) //map案例
    filterTransformation(sc) //filter案例
    flatMapTransformation(sc) //flatMap案例
    groupByKeyTransformation(sc) //groupByKey案例
    reduceByKeyTransformation(sc) //reduceByKey案例
    joinTransformation(sc) //join案例
    coGroupTransformation(sc) //cogroup案例
    sc.stop() //停止SparkContext，销毁相关的Driver对象，释放资源
  }
  def sparkContext(name: String) = {
    val conf = new SparkConf().setAppName(name).setMaster("local") //创建SparkConf初始化程序的配置
    val sc = new SparkContext(conf) //创建SparkContext，这是第一个RDD创建的唯一入口，也是Driver的灵魂，是通往集群的唯一通道
    sc
  }

  def mapTransformation(sc: SparkContext){
    val nums = sc.parallelize( 1 to 10) //根据集合创建RDD
    val mapped = nums.map(_ * 2) //map适用于任何类型的元素，且对其作用的集合中的每一个元素循环遍历并调用其作为参数的函数对每一个遍历的元素进行具体化处理
    mapped.collect().foreach(println) //收集计算结果并通过foreach循环打印
  }

  def filterTransformation(sc: SparkContext){
    val nums = sc.parallelize( 1 to 10) //根据集合创建RDD
    val filtered = nums.filter(_ % 2 == 0) //根据filter中作为参数的函数的Bool值来判断符合条件的元素，并基于这些元素构成新的MapPartitionsRDD
    filtered.collect().foreach(println) //收集计算结果并通过foreach循环打印
  }

  def flatMapTransformation(sc: SparkContext){
    val bigData = Array("Scala Spark", "Java Hadoop", "Java Tachyon") //实例化字符串类型的Array
    val bigDataStrings = sc.parallelize(bigData) //创建以字符串为元素类型的parallelCollectionRDD
    val words = bigDataStrings.flatMap(_.split(" ")) //首先是通过传入的作为参数的函数来作用于RDD的每个字符串进行单词切分（是以集合的方式存在的），然后把切分后的结果合并为一个大的集合产生结果为{Scala, Spark, Java, Hadoop, Java, Tachyon}
    words.collect().foreach(println) //收集计算结果并通过foreach循环打印
  }

  def groupByKeyTransformation(sc: SparkContext){
    val data = Array(Tuple2(100, "Spark"), Tuple2(100, "Tackyon"), Tuple2(70, "Hadoop"), Tuple2(80, "Kafka"), Tuple2(80, "HBase"))
    val dataRDD = sc.parallelize(data) //创建RDD
    val grouped = dataRDD.groupByKey() //按照相同的Key对Value进行分组，分组后的value是一个集合
    grouped.collect().foreach(println) //收集计算结果并通过foreach循环打印
  }

  def reduceByKeyTransformation(sc: SparkContext){

    val lines = sc.textFile("D://intellIJidea//ScalaProject//resource//helloSpark.txt",1)//读取本地文件并设置为1个Partition

    /**
      * 第四步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 第4.1步：将每一行的字符串拆分成单个的单词
      */
    val words = lines.flatMap(_.split(" ")) //对每一行的字符串进行单词拆分，并把所有行的拆分结果通过flat合并成一个大的单词集合

    /**
      * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
      */
    val pairs = words.map( word => (word, 1))

    /**
      * 第4.3步：在单词实例计数为1基础上，统计每个单词在文件中出现的总次数
      */

    val wordCounts = pairs.reduceByKey(_ + _) //对相同的key，进行Value的累加（包括Local和Reducer级别同时Reduce）

    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2)) //打印reduceByKey之后的计算结果

  }
  def joinTransformation(sc: SparkContext){
    val studentName = Array(
      Tuple2(1, "Spark"),
      Tuple2(2, "Tachyon"),
      Tuple2(3, "Hadoop")
    )

    val studentScores = Array(
      Tuple2(1, 100),
      Tuple2(2, 95),
      Tuple2(3, 65)
    )

    val names = sc.parallelize(studentName)
    val scores = sc.parallelize(studentScores)
    val studentNameAndScore = names.join(scores)
    studentNameAndScore.collect().foreach(println)
  }
  def coGroupTransformation(sc: SparkContext){
    val studentName = Array(
      Tuple2(1, "Spark"),
      Tuple2(2, "Tachyon"),
      Tuple2(3, "Hadoop"),
      Tuple2(1, "GOOD1"),
      Tuple2(2, "GOOD2"),
      Tuple2(3, "GOOD3")
    )

    val studentScores = Array(
      Tuple2(1, 100),
      Tuple2(2, 95),
      Tuple2(3, 60),
      Tuple2(1, 110),
      Tuple2(2, 90),
      Tuple2(3, 65)
    )

    val names = sc.parallelize(studentName)
    val scores = sc.parallelize(studentScores)
    val studentNameAndScore = names.cogroup(scores)
    studentNameAndScore.collect().foreach(println)
  }

}