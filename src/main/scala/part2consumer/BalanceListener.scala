package part2consumer

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import part2consumer.UtilObject.printRecord

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import scala.util.Try

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

  Try {
    kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"), new HandleBalance)
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100))
      records.forEach { record =>
        printRecord(record)
        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no meta"))
      }
      kafkaConsumer.commitAsync(currentOffsets, null)
    }
  }.recover {
    case _: WakeupException =>
    case e: Exception => println(s"Unexpected error: $e")
  }

  Try {
    kafkaConsumer.commitSync(currentOffsets)
  }
  kafkaConsumer.close()
  println("Consumer is closed")

}
