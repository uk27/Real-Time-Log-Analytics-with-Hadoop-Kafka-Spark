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
    
    
    private String brokers;
    private String topicName;
    private String fileName;
    private Producer<String, String> producer;
    
    
    public HdfsProducer(String brokers, String topicName, String fileName){
        
        this.topicName = topicName;
        this.brokers = brokers;
        this.fileName = fileName;
        this.producer = getProducer();
    }
    
    public void produce(){
        
        try{
            //1. Get the instance of Configuration
            Configuration configuration = new Configuration();
            //2. URI of the file to be read
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
            System.out.println("Producer exited successfully for " + fileName);
            
        }catch(Exception e){
            
            System.out.println("Exception while producing for " + fileName);
            System.out.println(e);
        }
    }
    
    private Producer<String, String> getProducer(){
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
    
}
