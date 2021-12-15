#First command line argument is path where Confluent Schema Registry is installed
#!/bin/bash
$1/bin/schema-registry-start $1/etc/schema-registry/schema-registry.properties
