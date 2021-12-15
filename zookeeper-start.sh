#First command line argument is path where Apache Kafka is installed
#!/bin/bash
$1/bin/zookeeper-server-start.sh $1/config/zookeeper.properties
