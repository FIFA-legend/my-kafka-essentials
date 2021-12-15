package part1producer.avro_serialization

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Try

object AvroSerialization extends App {

  // example using generic Avro objects
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put("schema.registry.url", "http://localhost:8081")

  val schemaInString =
    """
      |{
      |"namespace": "customerManagement.avro",
      |"type": "record",
      |"name": "Customer",
      |"fields": [
      |{"name": "id", "type": "int"},
      |{"name": "name", "type": "string"},
      |{"name": "email", "type": ["null", "string"], "default": null}]
      |}
      |""".stripMargin

  val producer = new KafkaProducer[String, GenericRecord](props)

  val parser = new Schema.Parser()
  val schema = parser.parse(schemaInString)

  val result = for {
    n <- 1 to 5
    name = "ExampleCustomer" + n
    email = "example" + n + "@example.com"
    customer = new GenericData.Record(schema)
    _ = customer.put("id", n)
    _ = customer.put("name", name)
    _ = customer.put("email", email)
    record = new ProducerRecord[String, GenericRecord]("CustomerContacts", name, customer)
    offsets = Try {
        producer.send(record).get.offset()
      }.toEither
        .left
        .map(_.getCause)
  } yield offsets

  println(result)

}
