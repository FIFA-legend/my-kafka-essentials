name := "my-kafka-essentials"

version := "0.1"

scalaVersion := "2.13.6"

val kafkaVersion = "3.0.0"
val avroVersion = "1.11.0"
val json4sVersion = "4.0.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "io.confluent" % "kafka-avro-serializer" % "7.0.0",
  "org.apache.avro" % "avro" % avroVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion
)

resolvers ++= Seq(
  "confluent".at("https://packages.confluent.io/maven/")
)

avroSource in Compile := (baseDirectory.value.getAbsoluteFile / "src/main/resources/avro")
avroStringType := "String"
Compile / packageAvro / publishArtifact := true