## Log Analytics with Hadoop, Kafka and Spark

### Overview
The customer runs a website and periodically is attacked by a botnet in a Distributed Denial of Service (DDOS) attack. We are given a log file in Apache log format from a given attack. Use this log to build a simple real-time detector of DDOS attacks. 

### Approach

Assuming the given files are on HDFS we first write a Kafka producer that reads HDFS directory continuously and polls for newly written log files. If it finds a file, it creates a corresponding Kafka topic and populates it by sending the file line by line. 

Next we write Kafka consumer. Instead of writing our own streaming application, we can use abstractions provided by stream processing frameworks. For our purpose we use Spark streaming. For each topic, we can start multiple consumer instances for a particular topic which can consume the messages in parallel. Also we can have separate group ids for different topics and hence utilize the combination of broadcast and message queuing feature of Kafka.

Further details can be found in "Log-Analytics.pdf"

### Prerequisites:

- The project is setup on the Cloudera Quickstart VM. We use the CDH5 [Download here](https://www.cloudera.com/downloads/quickstart_vms/5-12.html)

- We also need to download the [Cloudera distribution of Kafka](https://www.cloudera.com/documentation/kafka/latest/topics/kafka_packaging.html)
- Java libraries for Hadoop, Spark streaming, Kafka that are all inter-compatible. See pom.xml for details.

### Files

- *HdfsProduer.java*: Takes Kafka topic as a parameter, reads from log files on hdfs and send them line by line to the Kafka topic.


- *SparkConsumer.java*: Takes a list of broker nodes and a topic as parameters. Streams in the messages from the Kafka server in a configurable window and performs processing on them to find ips' of a DDoS attack


### Instructions

1) Start Kafka Servers on respective ports
```
sudo /usr/lib/kafka/bin/kafka-server-start.sh /usr/lib/kafka/config/server-<server-identifier>.properties
```
2) Create topic following the pattern: `topicName-1`, `topicName-2` etc to identify different topics of the same logical topic.
```
/usr/lib/kafka/bin/kafka-topics.sh --create --partitions 1 --topic topicx --zookeeper localhost:2181 --replication-factor 3
```
3) Make sure the hostname is correctly identified in /etc/hosts of the VM

4) Choose a new groupid


5) Run the Producer and Consumer using following commands:
```
java -jar HdfsProducer.jar localhost:9092 <directory-name>
java -jar SparkConsumer.jar <server-ip>:<server1-port>,<server2-ip>:<server2-port>,<server3-ip>:<server3-port> topicName groupId
```
### Optional:
- Use [Kafka Tool](http://www.kafkatool.com/index.html) for monitoring cluster
- Use Hue for viewing results


### Known issues:
1) Windowing: This worked with the 0-8 libraries.  https://issues.apache.org/jira/browse/SPARK-19185
2) Rebalancing partitions when new instances are added.  https://issues.apache.org/jira/browse/SPARK-19547
