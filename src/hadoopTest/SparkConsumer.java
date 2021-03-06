package hadoopTest.hadoopTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.regex.Pattern;
import java.text.*;

import scala.Tuple2;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.Durations;
//import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.kafka.common.serialization.StringDeserializer;


public class SparkConsumer {
	
	static Pattern p = null;
	private static Function2<Integer, Integer, Integer> MyReducerFunc = (a, b) -> a + b;
	
	public static void main(String[] args) throws Exception {
	    if (args.length < 3) {
	      System.err.println("Usage: SparkConsumer <brokers> <topic Pattern> <groupid>\n" +
	          "  <brokers> is a list of one or more Kafka brokers\n" +
	          "  <topic pattern> e.g. topicx will consume topics like topicx-1, topicx-99, etc\n\n" +
	          "	 <groupid>");
	      System.exit(1);
	    }
	    

	    String brokers = args[0];
	    String topicRegex = args[1]+"-\\d*";
	    String groupId = args[2];
	    
	   
	    Pattern p = Pattern.compile(topicRegex);
	    
	    // Create streaming context from spark context with required batch interval
	    SparkConf sparkConf = new  SparkConf().setMaster("local[10]").setAppName("SparkConsumer1").set("spark.driver.host", "localhost");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(20));	
	   
	    
	    Map<String, Object> kafkaParams = new HashMap<>();
	    kafkaParams.put("bootstrap.servers", brokers);
	    kafkaParams.put("auto.offset.reset", "latest");
	    kafkaParams.put("group.id", groupId);
	    kafkaParams.put("key.deserializer", StringDeserializer.class);
	    kafkaParams.put("value.deserializer", StringDeserializer.class);
	    //kafkaParams.put("zookeeper.connect", "192.168.101.165:2181");
	    kafkaParams.put("enable.auto.commit", "true");
	    kafkaParams.put("auto.commit.interval.ms", "1000");
	    kafkaParams.put("session.timeout.ms","20000");
	    kafkaParams.put("metadata.max.age.ms", "1000");
	    //kafkaParams.put("heartbeat.interval.ms", "8000");
	    //kafkaParams.put("max.poll.interval.ms", Integer.toString(Integer.MAX_VALUE));
	    
	    final JavaInputDStream<ConsumerRecord<String, String>> messages =
	    			KafkaUtils.createDirectStream(
	    			jssc,
	    			LocationStrategies.PreferBrokers(),
	    			ConsumerStrategies.SubscribePattern(p, kafkaParams)
	    		);
	    
	    JavaPairDStream<String, String> key_value = messages.mapToPair(
	    			new PairFunction<ConsumerRecord<String, String>, String, String>() {
	    			@Override
	    			public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
	    				return new Tuple2<>(record.key(), record.value());
	    		    }
	    		  });


		key_value.foreachRDD(rdd -> {

			long numHits = rdd.count();
			System.out.println("Number of partitions fetched: " + rdd.partitions().size());

			if(numHits < 1500)
				System.out.println("No new data fetched in last 30 sec");

			//Do Processing
			else{

				//Convert to java log object
				JavaRDD<ApacheAccessLog> logs =
						rdd.map(x-> x._2)
						.map(ApacheAccessLog::parseFromLogLine)
						.cache();

				//Find the bot ip addresses
				JavaRDD<String> iprdd = 
						logs.mapToPair(ip-> new Tuple2<>(ip.getIpAddress(),1))
						.reduceByKey(MyReducerFunc)
						.filter(botip-> botip._2 > 50)
						.keys();

				//If we find something, we store it in results dir on hdfs
				long botIpCount = iprdd.count();
				if(botIpCount > 0){

					//Set file configuration for hdfs
					sc.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
					sc.hadoopConfiguration().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

					//Combine the results and store
					String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
					iprdd.coalesce(1).saveAsTextFile("hdfs://quickstart.cloudera:8020/results/"+timeStamp);
					System.out.println("\n---------"+botIpCount+" Bot Ips were detected---------");
					System.out.println("\n---------Please see /results for details---------\n");
				}

			}

		});

		jssc.start();
		jssc.awaitTermination();
	}

}
