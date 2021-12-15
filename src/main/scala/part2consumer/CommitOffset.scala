package part2consumer

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import part2consumer.UtilObject.printRecord

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

object CommitOffset extends App {

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("group.id", "CountryCounter")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](kafkaProperties)
  kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"))

  // Commit Specified Offset
  val currentOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]
  var count = 0

  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))
    records.forEach { record =>
      printRecord(record)
      currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no meta"))
      if (count % 1000 == 0) kafkaConsumer.commitAsync(currentOffsets, null)
      count = count + 1
    }
  }

}
