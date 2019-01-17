package com.shursulei.spark.streaming.byJava;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SparkStreamingDataManuallyProducerForKafkas extends Thread{
		//具体的论坛频道
		static String[] channelNames = new  String[]{
			"Spark","Scala","Kafka","Flink","Hadoop","Storm",
			"Hive","Impala","HBase","ML"
		};
		//用户的两种行为模式
		static String[] actionNames = new String[]{"View", "Register"};
		private static Producer<String, String> producerForKafka;
		private static String dateToday;
		private static Random random;
		//2、作为线程而言，要复写run方法，先写业务逻辑，再写控制
		@Override
		public void run() {
			int counter = 0;//搞500条
			while(true){//模拟实际情况，不断循环，异步过程，不可能是同步过程
			   counter++;
			  String userLog = userlogs();
			  System.out.println("product:"+userLog);
			  //"test"为topic
			  producerForKafka.send(new KeyedMessage<String, String>("test", userLog));
			  if(0 == counter%500){
					counter = 0;
					try {
					   Thread.sleep(1000);
					} catch (InterruptedException e) {
					   // TODO Auto-generated catch block
					   e.printStackTrace();
					}
				}
			}
		}
			
		private static String userlogs() {
			StringBuffer userLogBuffer = new StringBuffer("");
			int[] unregisteredUsers = new int[]{1, 2, 3, 4, 5, 6, 7, 8};
			long timestamp = new Date().getTime();
				Long userID = 0L;
				long pageID = 0L;
				//随机生成的用户ID 
				if(unregisteredUsers[random.nextInt(8)] == 1) {
				   userID = null;
				} else {
				   userID = (long) random.nextInt((int) 2000);
				}
				//随机生成的页面ID
				pageID =  random.nextInt((int) 2000);          
				//随机生成Channel
				String channel = channelNames[random.nextInt(10)];
				//随机生成action行为
				String action = actionNames[random.nextInt(2)];
				
				userLogBuffer.append(dateToday)
							.append("\t")
							.append(timestamp)
							.append("\t")
							.append(userID)
							.append("\t")
							.append(pageID)
							.append("\t")
							.append(channel)
							.append("\t")
							.append(action);   //这里不要加\n换行符，因为kafka自己会换行，再append一个换行符，消费者那边就会处理不出数据
			return userLogBuffer.toString();
		}
	    
		public static void main(String[] args) throws Exception {
		  dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		  random = new Random();
			Properties props = new Properties();
			props.put("zk.connect", "h71:2181,h72:2181,h73:2181");
			props.put("metadata.broker.list","h71:9092,h72:9092,h73:9092");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			ProducerConfig config = new ProducerConfig(props);
			producerForKafka = new Producer<String, String>(config);
			new SparkStreamingDataManuallyProducerForKafkas().start(); 
		}
}
