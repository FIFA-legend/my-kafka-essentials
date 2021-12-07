package part2consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util.{Collections, Properties}
import scala.util.Try

object CorrectExit extends App {

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("group.id", "CountryCounter")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](kafkaProperties)

  val mainThread = Thread.currentThread()

  /*new Thread() {
    override def run(): Unit = {
      println("Starting exit from Kafka Consumer...")
      Thread.sleep(10000)
      kafkaConsumer.wakeup()
    }
  }.start()*/

  Runtime.getRuntime.addShutdownHook(
    new Thread() {
      override def start(): Unit = {
        println("Starting exit from Kafka Consumer...")
        kafkaConsumer.wakeup()
        Try(mainThread.join())
          .toEither
          .left
          .map(_.printStackTrace())
      }
    }
  )

  try {
    kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"))
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100))
      records.forEach { record =>
        println(s"offset = ${record.offset}, key = ${record.key}, value = ${record.value}")
      }
      kafkaConsumer.commitSync()
    }
  } catch {
    case _: WakeupException => println("Wake up method is caught")
    case e: Exception => println(s"Unexpected error: $e")
  } finally {
      kafkaConsumer.close()
      println("Consumer is finally closed")
  }

}
