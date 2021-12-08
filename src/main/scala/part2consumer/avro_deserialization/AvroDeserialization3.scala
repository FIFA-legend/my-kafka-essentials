package part2consumer.avro_deserialization

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import part1producer.avro_serialization.Customer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala

object AvroDeserialization3 extends App {

  // Deserialization out of GenericRecord
  val properties = new Properties()

  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "AvroDeserialization3")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
  properties.put("schema.registry.url", "http://localhost:8081")
  properties.put("auto.offset.reset", "earliest")
  properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)

  val topic = "customer-avro"

  val consumer = new KafkaConsumer[String, Customer](properties)
  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(Duration.ofMillis(100)).asScala
    records.foreach {
      record =>
        val customer = record.value()
        println(s"Key: ${record.key()}, Customer: $customer")
    }
  }

}
