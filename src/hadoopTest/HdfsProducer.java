package hadoopTest.hadoopTest;
//import util.properties packages
import java.io.*;
import java.util.*;
import java.net.*;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//import Hadoop Packages
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class HdfsProducer {

	public static void readFromHdfs(Producer<String, String> producer, String topicName, String fileName) {

		try{
			//1. Get the instance of Configuration
			Configuration configuration = new Configuration();
			//2. URI of the file to be read
			//URI uri = new URI("hdfs://0.0.0.0:8022/data/apache-access-log.txt");
			URI uri = new URI(fileName);
			//3. Get the instance of the HDFS 
			FileSystem hdfs = FileSystem.get(uri, configuration);
			Path pt = new Path(uri);
			BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
			String line;
			
			line=br.readLine();
			int count = 1;
			//while (line != null){
			while (line != null && count <10){
				
				System.out.println("Sending batch" + count);
				producer.send(new ProducerRecord<String, String>(topicName, new String(line)));
				line=br.readLine();
				count = count+1;				
			}
			
			producer.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static Producer<String, String> getProducer(String brokers, String topicName)throws Exception{
		
		// create instance for properties to access producer configs   
				Properties props = new Properties();

				//Assign localhost id
				props.put("bootstrap.servers", brokers);
				
				props.put("auto.create.topics.enable", "true");

				//Set acknowledgements for producer requests.      
				props.put("acks", "all");

				//If the request fails, the producer can automatically retry,
				props.put("retries", 0);

				//Specify buffer size in config
				props.put("batch.size", 16384);

				//Reduce the no of requests less than 0   
				props.put("linger.ms", 1);

				//The buffer.memory controls the total amount of memory available to the producer for buffering.   
				props.put("buffer.memory", 33554432);

				props.put("key.serializer", 
						"org.apache.kafka.common.serialization.StringSerializer");

				props.put("value.serializer", 
						"org.apache.kafka.common.serialization.StringSerializer");
				
				props.put("topic.metadata.refresh.interval.ms", "10");

				Producer<String, String> producer = new KafkaProducer
						<String, String>(props);
				
				return producer;
	}

	
	public static void init(String brokers, String topicName, String fileName){
		try {
			readFromHdfs(getProducer(brokers, topicName), topicName, fileName);
			System.out.println("Producer exited normally for "+fileName);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while producing messages for "+fileName+"\n" +e);
		}
	}
}
