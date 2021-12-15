package part1producer.custom_serialization

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Try

object CustomSerialization extends App {

  // Creating custom serializers
  case class SimpleCustomer(customerId: Int, customerName: String)

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // To specify a custom class here, you need to place the class in a separate file (not inside another class or object)
  kafkaProperties.put("value.serializer", "part1producer.custom_serialization.CustomerSerializer")

  val producer = new KafkaProducer[String, SimpleCustomer](kafkaProperties)
  val record = new ProducerRecord[String, SimpleCustomer]("Customers", "Customer Objects", SimpleCustomer(1, "Nikita"))

  Try {
    println(producer.send(record).get.offset())
  }.recover {
    case e: Exception => e.printStackTrace()
  }

}
