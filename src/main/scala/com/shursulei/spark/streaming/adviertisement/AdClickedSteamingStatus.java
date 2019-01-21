package com.shursulei.spark.streaming.adviertisement;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka.KafkaUtils;
public class AdClickedSteamingStatus {
	public static void main(String[] args) {
	/*第105课：  Spark Streaming电商广告点击综合案例在线点击统计实战
	 * 广告点击的基本数据格式：timestamp,ip,userID,adID,province,city
	 * 时间、ip、用户ID、广告ID，点击广告所在的省、所在的城市
	 * 至少2条线程，一条线程接受数据，一条处理数据
	 * 每个executor 一般分配 多少core？5个core最佳的 分配为奇数个core表现最佳 3个 5个 7个
	 *
	 */	
	SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingWordCountOnline");
	JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));
	Map kafaParameters = new HashMap();
	kafaParameters.put("metadata.broker.list", 
			"master:9092,worker1:9092,worker2:9092");
	Set topics =new HashSet();
	topics.add("AdClicked");
	
	JavaPairInputDStream adClickedStreaming =KafkaUtils.createDirectStream(jsc,
			String.class,String.class,
			StringDecoder.class, StringDecoder.class, 
			kafaParameters,
			topics);
		 
	    JavaPairDStream pairs = adClickedStreaming.mapToPair(new PairFunction, String, Long>() {

			@Override
			public Tuple2 call(Tuple2 t) throws Exception {
				 String[] splited=t._2.split("\t");
				 String timestamp = splited[0];
				 String ip = splited[1];
				 String userID = splited[2];
				 String adID = splited[3];
				 String province = splited[4];
				 String city = splited[5];		
				 
				 String clickedRecord = timestamp + "_" +ip + "_"+userID+"_"+adID+"_"
						     +province +"_"+city;
				 return new Tuple2(clickedRecord, 1L);
			}
		});
        //new Function2
	    JavaPairDStream adClickedUsers= pairs.reduceByKey(new Function2() {
	      @Override
	      public Long call(Long i1, Long i2) throws Exception{
	        return i1 + i2;
	      }
	    });

	    /*判断有效的点击，复杂化的采用机器学习训练模型进行在线过滤    简单的根据ip判断1天不超过100次；也可以通过一个batch duration的点击次数
			判断是否非法广告点击，通过一个batch来判断是不完整的，还需要一天的数据也可以每一个小时来判断。*/
	    JavaPairDStream    filterClickedBatch = adClickedUsers.filter(new Function, Boolean>() {
			@Override
			public Boolean call(Tuple2 v1) throws Exception {
			if (1 < v1._2){
				return false;
			} else { 
				return true;
					}
			}
		});
	    filterClickedBatch.print();
	    jsc.start();
	    jsc.awaitTermination();
	    jsc.close();
	while (true ){	
	}
	}
}
