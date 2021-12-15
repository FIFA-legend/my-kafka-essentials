package part1producer.avro_serialization

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties

object AvroSerialization3 extends App {

  val properties = new Properties()

  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("acks", "1")
  properties.setProperty("retries", "10")
  properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
  properties.setProperty("schema.registry.url", "http://localhost:8081")

  val producer = new KafkaProducer[String, Customer](properties)
  val topic = "customer-avro"

  val customer = Customer.newBuilder()
    .setFirstName("Nikita")
    .setLastName("Kolodko")
    .setAge(20)
    .setHeight(175.5.toFloat)
    .setWeight(65.2.toFloat)
    .setAutomatedEmail(false)
    .build()

  val record = new ProducerRecord[String, Customer](topic, customer)
  producer.send(record, (metadata: RecordMetadata, e: Exception) => {
    if (e == null) {
      println(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, Offset: ${metadata.offset()}, Timestamp: ${metadata.timestamp()}")
    } else {
      e.printStackTrace()
    }
  })

  producer.flush()
  producer.close()

}
