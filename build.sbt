name := "my-kafka-essentials"

version := "0.1"

scalaVersion := "2.13.6"

val kafkaVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "io.confluent" % "kafka-avro-serializer" % "7.0.0"
)

resolvers ++= Seq(
  "confluent".at("https://packages.confluent.io/maven/")
)