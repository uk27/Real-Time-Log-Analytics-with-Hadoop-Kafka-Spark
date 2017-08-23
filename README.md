# Real-Time-Log-Analytics-with-Hadoop-Kafka-Spark

HdfsProduer.java: Takes Kafka topic as a parameter, reads from log files on hdfs and send them line by line to the Kafka topic.


SparkConsumer.java: Takes a list of broker nodes and a topic as parameters. Streams in the messages from the Kafka server in a configurable window and performs processing on them to find ips' of a DDoS attack
