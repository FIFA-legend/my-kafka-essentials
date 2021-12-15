package part2consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

object UtilObject {

  def printRecord[T](record: ConsumerRecord[String, T]): Unit = {
    println(s"topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, customer = ${record.key}, country = ${record.value}")
  }

}
