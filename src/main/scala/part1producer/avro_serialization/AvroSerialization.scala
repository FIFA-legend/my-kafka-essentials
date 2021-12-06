package part1producer.avro_serialization

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Try

object AvroSerialization extends App {

  // example using generic Avro objects
  case class Customer(customerId: Int, customerName: String, email: String)

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
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

  for {
    n <- 1 to 5
    name = "ExampleCustomer" + n
    email = "example" + n + "@example.com"
    customer = new GenericData.Record(schema)
    _ = customer.put("id", n)
    _ = customer.put("name", name)
    _ = customer.put("email", email)
    record = new ProducerRecord[String, GenericRecord]("customerContacts", name, customer)
    _ = Try(producer.send(record).get).toEither match {
      case Left(e) => e.printStackTrace()
      case Right(value) => println(value.offset())
    }
  } yield ()

}
