package part2consumer.custom_deserialization

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.{Collections, Properties}

object CustomDeserialization extends App {

  case class Customer(customerId: Int, customerName: String)

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "part2consumer.custom_deserialization.CustomDeserializer")
  kafkaProperties.put("group.id", "Customers")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, Customer](kafkaProperties)
  kafkaConsumer.subscribe(Collections.singletonList("Customers"))

  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))
    records.forEach { record =>
      println(s"Customer ID: ${record.value().customerId}, Customer Name: ${record.value().customerName}")
    }
  }

}
