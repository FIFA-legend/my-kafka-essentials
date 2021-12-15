package part2consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import part2consumer.UtilObject.printRecord

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

object SyncAndAsyncCommit extends App {

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("group.id", "CountryCounter")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](kafkaProperties)
  kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"))

  // Combining Synchronous and Asynchronous Commits to provide safe commit
  Try {
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100)).asScala
      records.foreach(printRecord)
      kafkaConsumer.commitAsync()
    }
  }.recover {
    case e: Exception => println(s"Unexpected error: $e")
  }
  Try(kafkaConsumer.commitSync())
  kafkaConsumer.close()

}
