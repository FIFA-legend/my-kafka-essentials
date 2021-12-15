package part2consumer.avro_deserialization

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala

object AvroDeserialization extends App {

  // Deserialization out of GenericRecord
  case class CustomerContacts(id: Int, name: String, email: String)

  val properties = new Properties()

  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "AvroDeserialization")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
  properties.put("schema.registry.url", "http://localhost:8081")
  properties.put("auto.offset.reset", "earliest")

  val topic = "CustomerContacts"

  val consumer = new KafkaConsumer[String, GenericRecord](properties)
  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(Duration.ofMillis(100)).asScala
    records.foreach {
      record =>
        val genericRecord = record.value()
        val id = genericRecord.get("id")
        val name = genericRecord.get("name")
        val email = genericRecord.get("email")
        val customer = CustomerContacts(id.toString.toInt, name.toString, email.toString)
        println(s"Key: ${record.key()}, Customer: $customer")
    }
  }

}
