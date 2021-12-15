#First command line argument is path where Apache Kafka is installed
#!/bin/bash
$1/bin/kafka-server-start.sh $1/config/server.properties
