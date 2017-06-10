package hadoopTest.hadoopTest;
//import util.properties packages
import java.io.*;
import java.util.*;
import java.net.*;

//import Hadoop Packages
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Driver {

	public static Set<String> fileSet = new HashSet<String>();
	public static Map<String, HashSet<String>> fileMap = new HashMap<String, HashSet<String>>();
	public static void main(String args[]){
	
		// Check arguments length value
		if(args.length < 3){
			System.out.println("Usage: HdfsProducer <brokers> <Path on hdfs> <topic>");
			return;
		}

		
	while(true){
		try{
		Configuration configuration = new Configuration();
		
		URI uri = new URI("hdfs://0.0.0.0:8022");
		
		FileSystem fs = FileSystem.get(uri, configuration);

	    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
	            new Path(args[1]), true);
	    
	    while(fileStatusListIterator.hasNext()){
	        
	    	String fileName = fileStatusListIterator.next().getPath().toString();
	    	String segments[] = fileName.split("/");
	    	String topic = segments[segments.length-2];
	    	
	        if(!fileMap.containsKey(topic) || !(fileMap.get(topic).contains(fileName))){
	        	
	        	HashSet<String> temp = fileMap.get(topic);
	        	if(temp == null)
	        		temp = new HashSet<String>();
	        	
	        	temp.add(fileName);
	        	fileMap.put(topic, temp);
	        	
	        	//Create a producer thread
	        	System.out.println("Creating a new thread for topic "+topic);
	        	System.out.println("Filename "+fileName);
	        	try {
	        	     
	        	      Runnable producerThread = new Runnable() {
	        	        public void run() {
	        	        	
	        	        	HdfsProducer.init(args[0], topic, fileName);
	        	        }
	        	      };

	        	      new Thread(producerThread).start();
	        	    } catch (Exception x) {
	        	      x.printStackTrace();
	        	    }
	        }
	        
	    }
	    
	    Thread.sleep(3000);
	}
	catch(Exception e)
	{
		System.out.println(e);
		System.exit(1);
	}
	}
	}
}
