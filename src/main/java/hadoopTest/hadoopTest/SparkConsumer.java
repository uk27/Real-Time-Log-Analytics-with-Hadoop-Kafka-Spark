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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public class SparkConsumer {
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
	    //SparkConf sparkConf = new SparkConf().setAppName("SparkConsumer").setMaster("local[2]").set("spark.executor.memory","1g");
	    SparkConf sparkConf = new  SparkConf().setMaster("local[2]").setAppName("SparkConsumer").set("spark.driver.host", "localhost");
//	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    // Create a StreamingContext with a 1 second batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));	
	    
	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", brokers);
	    kafkaParams.put("auto.offset.reset", "largest");
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

	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      @Override
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });
	    
	    lines.print(); 
	    
	    jssc.start();
	    jssc.awaitTermination();
}
}
