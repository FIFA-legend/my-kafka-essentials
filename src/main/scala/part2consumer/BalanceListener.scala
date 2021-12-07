package part2consumer

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

object BalanceListener extends App {

  val currentOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]

  class HandleBalance extends ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      println("Lost partitions in rebalance. Commiting current offsets: " + currentOffsets)
      kafkaConsumer.commitSync(currentOffsets)
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {}
  }

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("group.id", "CountryCounter")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](kafkaProperties)

  try {
    kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"), new HandleBalance)
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100))
      records.forEach { record =>
        println(s"topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, customer = ${record.key}, country = ${record.value}")
        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no meta"))
      }
      kafkaConsumer.commitAsync(currentOffsets, null)
    }
  } catch {
    case _: WakeupException =>
    case e: Exception => println(s"Unexpected error: $e")
  } finally {
    try {
      kafkaConsumer.commitSync(currentOffsets)
    } finally {
      kafkaConsumer.close()
      println("Consumer is closed")
    }
  }

}
