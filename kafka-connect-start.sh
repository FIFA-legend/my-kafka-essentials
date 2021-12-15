#First command line argument is path where Apache Kafka is installed
#!/bin/bash
$1/bin/connect-distributed.sh $1/config/connect-distributed.properties
