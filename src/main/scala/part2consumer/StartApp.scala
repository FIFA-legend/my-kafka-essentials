package part2consumer

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

object StartApp extends App {

  val kafkaProperties = new Properties()

  kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9092")
  kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.put("group.id", "CountryCounter")
  kafkaProperties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](kafkaProperties)
  kafkaConsumer.subscribe(Collections.singletonList("CustomerCountry"))

  import org.json4s._
  import org.json4s.native.Serialization._
  import org.json4s.native.Serialization

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  val customerCountryMap = scala.collection.mutable.Map.empty[String, Int]
  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))
    records.forEach { record =>
      println(s"topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, customer = ${record.key}, country = ${record.value}")
      customerCountryMap.put(record.value(), customerCountryMap.getOrElse(record.value(), 0) + 1)

      val json = write(customerCountryMap)
      println(json)
    }
    // commit synchronously last message received by poll()
    // kafkaConsumer.commitSync()

    // commit asynchronously last message received by poll()
    // kafkaConsumer.commitAsync()

    // commit asynchronously last message received by poll() with callback on error
    //    kafkaConsumer.commitAsync(
    //      (offsets, e) => {
    //        if (e != null) {
    //          println(s"Failed commit for offset: $offsets")
    //          e.printStackTrace()
    //        }
    //      }
    //    )
  }

  // You can’t have multiple consumers that belong to the same group in one thread
  // and you can’t have multiple threads safely use the same consumer. One consumer per thread is the rule.
  // To run multiple consumers in the same group in one application, you will need to run each in its own thread.
  // It is useful to wrap the consumer logic in its own object and then use Java’s ExecutorService
  // to start multiple threads each with its own consumer.

  // One more example of code
  //  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProperties)
  //  consumer.subscribe(util.Arrays.asList("CustomerCountry"))
  //  while (true) {
  //    val record = consumer.poll(1000).asScala
  //    for (data <- record.iterator)
  //      println(data.value())
  //  }

  // fetch.min.bytes
  // This property allows a consumer to specify the minimum amount of data
  // that it wants to receive from the broker when fetching records.
  // fetch.max.wait.ms
  // By setting fetch.min.bytes, you tell Kafka to wait until it has enough data to send
  // before responding to the consumer.
  // If you set fetch.max.wait.ms to 100 ms and fetch.min.bytes to 1 MB,
  // Kafka will receive a fetch request from the consumer and will respond with data
  // either when it has 1 MB of data to return or after 100 ms, whichever happens first.
  // max.partition.fetch.bytes
  // This property controls the maximum number of bytes the server will return per partition. The default is 1 MB.
  // So if a topic has 20 partitions, and you have 5 consumers, each consumer will need to have 4 MB of memory available for ConsumerRecords.
  // In practice, you will want to allocate more memory as each consumer will need to handle more partitions if other consumers in the group fail.
  // session.timeout.ms
  // The amount of time a consumer can be out of contact with the brokers while still considered alive defaults to 3 seconds.
  // auto.offset.reset
  // This property controls the behavior of the consumer when it starts reading a partition
  // for which it does not have a committed offset or if the committed offset it has is invalid
  // The default is “latest,” which means that lacking a valid offset, the consumer will start reading from the newest records (records that
  // were written after the consumer started running). The alternative is “earliest,” which means
  // that lacking a valid offset, the consumer will read all the data in the partition, starting from the very beginning.
  // enable.auto.commit
  // This parameter controls whether the consumer will commit offsets automatically, and defaults to true.
  // partition.assignment.strategy
  // By default, Kafka has two assignment strategies:
  // Range
  // Assigns to each consumer a consecutive subset of partitions from each topic it subscribes to.
  // So if consumers C1 and C2 are subscribed to two topics, T1 and T2, and each of the topics has three partitions,
  // then C1 will be assigned partitions 0 and 1 from topics T1 and T2, while C2 will be assigned partition 2 from those topics.
  // Because each topic has an uneven number of partitions and the assignment is done for each topic independently,
  // the first consumer ends up with more partitions than the second.
  // RoundRobin
  // Takes all the partitions from all subscribed topics and assigns them to consumers sequentially, one by one.
  // If C1 and C2 described previously used RoundRobin assignment, C1 would have partitions 0 and 2 from topic T1 and partition 1 from topic T2.
  // C2 would have partition 1 from topic T1 and partitions 0 and 2 from topic T2. In general, if all consumers are subscribed to the same topics,
  // RoundRobin assignment will end up with all consumers having the same number of partitions (or at most 1 partition difference).
  // The partition.assignment.strategy allows you to choose a partition-assignment strategy.
  // The default is org.apache.kafka.clients.consumer.RangeAssignor, which implements the Range strategy described above.
  // You can replace it with org.apache.kafka.clients.consumer.RoundRobinAssignor.
  // A more advanced option is to implement your own assignment strategy, in which case partition.assignment.strategy should point to the name of your class.
  // client.id
  // This can be any string, and will be used by the brokers to identify messages sent from the client.
  // max.poll.records
  // This controls the maximum number of records that a single call to poll() will return.
  // receive.buffer.bytes, send.buffer.bytes
  // These are the sizes of the TCP send and receive buffers used by the sockets when writing and reading data.
  // If these are set to -1, the OS defaults will be used.

  // Combining Synchronous and Asynchronous Commits
  try {
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100)).asScala
      for {
        record <- records
        _ = println(s"topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, customer = ${record.key}, country = ${record.value}")
      } yield ()
      kafkaConsumer.commitAsync()
    }
  } catch {
    case e: Exception => println(s"Unexpected error: $e")
  } finally {
    try {
      kafkaConsumer.commitSync()
    } finally {
      kafkaConsumer.close()
    }
  }

  // Commit Specified Offset
  val currentOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]
  var count = 0

  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))
    records.forEach { record =>
      println(s"topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, customer = ${record.key}, country = ${record.value}")
      customerCountryMap.put(record.value(), customerCountryMap.getOrElse(record.value(), 0) + 1)

      currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no meta"))
      if (count % 1000 == 0) kafkaConsumer.commitAsync(currentOffsets, null)
      count = count + 1
    }
  }

}
