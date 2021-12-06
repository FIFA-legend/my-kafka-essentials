package part1producer.custom_serialization

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Try

object CustomSerialization extends App {

  // Creating custom serializers
  case class Customer(customerId: Int, customerName: String)

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // To specify a custom class here, you need to place the class in a separate file (not inside another class or object)
  kafkaProperties.put("value.serializer", "part1producer.custom_serialization.CustomerSerializer")

  val producer = new KafkaProducer[String, Customer](kafkaProperties)
  val record = new ProducerRecord[String, Customer]("Customers", "Customer Objects", Customer(1, "Nikita"))

  Try(producer.send(record).get).toEither match {
    case Left(e) => e.printStackTrace()
    case Right(value) => println(value.offset())
  }

}
