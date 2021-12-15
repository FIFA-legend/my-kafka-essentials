#!/bin/bash
curl http://localhost:8083/connector-plugins
echo '{"name":"mysql-login-connector", "config":{"connector.class":"JdbcSourceConnector","connection.url":"jdbc:mysql://127.0.0.1:3306/test?user=mikita.kalodka&password=0987654321KnKn","mode":"timestamp","table.whitelist":"login","validate.non.null":false,"timestamp.column.name":"login_time","topic.prefix":"mysql."}}' | curl -X POST -d @- http://localhost:8083/connectors --header "content-Type:application/json"
echo '{"name":"elastic-login-connector", "config":{"connector.class":"ElasticsearchSinkConnector","connection.url":"http://localhost:9200","type.name":"mysql-data","topics":"mysql.login","key.ignore":true}}' | curl -X POST -d @- http://localhost:8083/connectors --header "Content-Type:application/json"
sleep 1
curl -s -X "GET" "http://localhost:9200/mysql.login/_search?pretty=true"
curl -X DELETE http://localhost:8083/connectors/mysql-login-connector
curl -X DELETE http://localhost:8083/connectors/elastic-login-connector
