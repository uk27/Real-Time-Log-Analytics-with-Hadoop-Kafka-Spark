package hadoopTest.hadoopTest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.sql.api.java.JavaSQLContext;

import com.sun.tools.javac.util.List;

public class SparkConsumer {
	
	private static Function2<Integer, Integer, Integer> MyReducerFunc = (a, b) -> a + b;
	
	public static void main(String[] args) throws Exception {
	    if (args.length < 2) {
	      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
	          "  <brokers> is a list of one or more Kafka brokers\n" +
	          "  <topics> is a list of one or more kafka topics to consume from\n\n");
	      System.exit(1);
	    }

	    //StreamingExamples.setStreamingLogLevels();

	    String brokers = args[0];
	    String topics = args[1];

	    // Create context with a 2 seconds batch interval
	    
	    SparkConf sparkConf = new  SparkConf().setMaster("local[4]").setAppName("SparkConsumer").set("spark.driver.host", "localhost");
	    
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
	    // Create a StreamingContext with a 1 second batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(2));	
	    
	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    
	    Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", brokers);
	    //kafkaParams.put("auto.offset.reset", "smallest");
	    kafkaParams.put("group.id", "SparkConsumerGrp");
	    
	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
	    
	    JavaPairDStream<String, String> messages2 =
	            messages.window(Durations.seconds(30), Durations.seconds(30));
	    
	    
	    messages2.foreachRDD(rdd -> {
	    	
	    	long numHits = rdd.count();
	    	
	    	if(numHits == 0)
	    		System.out.println("No new data fetched in last 30 sec");
	    	
	    	//Do Processing
	    	else{
	    			System.out.println("Data fetched in the last 30 seconds: " + rdd.partitions().size()
	    					+ " partitions and " + numHits  + " records");
	        
	    			//Convert to java log object	
	    			JavaRDD<ApacheAccessLog> logs = rdd.map(x-> x._2)
	    												.map(ApacheAccessLog::parseFromLogLine)
	    													.cache();
	        
	    			//Find the bot ip addresses
	    			JavaRDD<String> iprdd =  logs.mapToPair(ip-> new Tuple2<>(ip.getIpAddress(),1))
	    					.reduceByKey(MyReducerFunc)
	    						.filter(botip-> botip._2 > 50)
	    							.keys();
	    			
	    			//If we find something, we store it in results dir on hdfs
	    			if(iprdd.count() > 0)
	    				iprdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/results/");
	    			
	    			System.out.println("\n-------------Resuts successfully written to /results on hdfs-------------\n");
	    		}        
	        
	    });
	    
	   
	  
	    jssc.start();
	    jssc.awaitTermination();
}
}
