package part1producer

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import scala.util.Try

object StartApp extends App {

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](kafkaProperties)

  // types of record must match our serializer and producer types
  val record = new ProducerRecord[String, String]("CustomerCountry", "Precision Products", "France")

  //  This method is used when dropping a message is acceptable (fire-and-forget)
  //  Try(producer.send(record)).toEither match {
  //    case Left(e) => e.printStackTrace()
  //    case Right(value) => println(value.toString)
  //  }

  // Sending message synchronously
  // There are 2 types of exceptions in Kafka: retriable and non-retriable
  //  Try(producer.send(record).get).toEither match {
  //    case Left(e) => e.printStackTrace()
  //    case Right(value) => println(value.offset())
  //  }

  // Sending message asynchronously
  // Second argument in method send() must implement Callback class with single method onCompletion()
  case class DemoProducerCallback() extends Callback {
    override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) e.printStackTrace()
      else println(s"Message offset: ${metadata.offset()}")
    }
  }

  val record2 = new ProducerRecord[String, String]("CustomerCountry", "Biomedical Materials", "USA")
  producer.send(record2, DemoProducerCallback())

  // Additional configuration properties
  // acks
  // If acks=0 , the producer will not wait for a reply from the broker before assuming the message was sent successfully (high throughput, low safety).
  // If acks=1 , the producer will receive a success response from the broker the moment the leader replica received the message.
  // If acks=all , the producer will receive a success response from the broker once all in-sync replicas received the message (high safety, long latency).
  // buffer.memory
  // This sets the amount of memory the producer will use to buffer messages waiting to be sent to brokers.
  // compression.type
  // By default, messages are sent uncompressed. This parameter can be set to snappy, gzip or lz4
  // Snappy compression provides decent compression ratios with low CPU overhead and good performance,
  // so it is recommended in cases where both performance and bandwidth are a concern.
  // Gzip compression will typically use more CPU and time but result in better compression ratios,
  // so it recommended in cases where network bandwidth is more restricted.
  // retries
  // The value of the retries parameter will control how many times the producer will retry sending the message
  // before giving up and notifying the client of an issue.
  // batch.size
  // This parameter controls the amount of memory in bytes (not messages!) that will be used for each batch.
  // When the batch is full, all the messages in the batch will be sent.
  // However, this does not mean that the producer will wait for the batch to become full.
  // linger.ms
  // linger.ms controls the amount of time to wait for additional messages before sending the current batch.
  // KafkaProducer sends a batch of messages either when the current batch is full or when the linger.ms limit is reached.
  // By setting linger.ms higher than 0, we instruct the producer to wait a few milliseconds to add additional messages to the batch
  // before sending it to the brokers.
  // client.id
  // This can be any string, and will be used by the brokers to identify messages sent from the client.
  // max.in.flight.requests.per.connection
  // This controls how many messages the producer will send to the server without receiving responses.
  // Setting this high can increase memory usage while improving throughput,
  // but setting it too high can reduce throughput as batching becomes less efficient.
  // Setting this to 1 will guarantee that messages will be written to the broker in the order in which they were sent, even when retries occur.
  // timeout.ms, request.timeout.ms, metadata.fetch.timeout.ms
  // These parameters control how long the producer will wait for a reply from the server when sending data (request.timeout.ms)
  // and when requesting metadata such as the current leaders for the partitions we are writing to (metadata.fetch.timeout.ms).
  // timeout.ms controls the time the broker will wait for in-sync replicas to acknowledge the message in order to meet the acks configuration —
  // the broker will return an error if the time elapses without the necessary acknowledgments.
  // max.block.ms
  // This parameter controls how long the producer will block when calling send() and when explicitly requesting metadata via partitionsFor().
  // max.request.size
  // This setting controls the size of a produce request sent by the producer. It caps both the size of the largest message that can be sent
  // and the number of messages that the producer can send in one request.
  // receive.buffer.bytes and send.buffer.bytes
  // These are the sizes of the TCP send and receive buffers used by the sockets when writing and reading data.

}
