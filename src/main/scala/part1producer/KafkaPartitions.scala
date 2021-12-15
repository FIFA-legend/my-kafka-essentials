package part1producer

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Cluster, InvalidRecordException}

import java.util

object KafkaPartitions extends App {

  // When the key is null and the default partitioner is used, the record will be sent to
  // one of the available partitions of the topic at random. A round-robin algorithm will
  // be used to balance the messages among the partitions.
  // If a key exists and the default partitioner is used, Kafka will hash the key (using its
  // own hash algorithm, so hash values will not change when Java is upgraded), and use
  // the result to map the message to a specific partition.
  // The mapping of keys to partitions is consistent only as long as the number of partitions in a topic does not change.

  // Partitioner interface includes configure , partition , and close methods.

  // Custom partitioning
  class BananaPartitioner extends Partitioner {
    override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
      val partitions = cluster.partitionsForTopic(topic)
      val partitionsNumber = partitions.size()

      if ((keyBytes == null) || (!key.isInstanceOf[String]))
        throw new InvalidRecordException("Name should be a string")

      if (key.asInstanceOf[String] == "Banana") partitionsNumber - 1
      else Math.abs(Utils.murmur2(keyBytes)) % (partitionsNumber - 1)
    }

    override def close(): Unit = {}

    override def configure(configs: util.Map[String, _]): Unit = {}
  }

}
