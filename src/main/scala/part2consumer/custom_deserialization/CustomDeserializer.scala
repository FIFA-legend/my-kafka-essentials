package part2consumer.custom_deserialization

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import part2consumer.custom_deserialization.CustomDeserialization.SimpleCustomer

import java.nio.ByteBuffer

class CustomDeserializer extends Deserializer[SimpleCustomer] {
  override def deserialize(topic: String, data: Array[Byte]): SimpleCustomer = {
    if (data == null) null
    else if (data.length < 8) throw new SerializationException("Size of data is shorter than expected")
    else {
      val buffer = ByteBuffer.wrap(data)
      val id = buffer.getInt
      val nameSize = buffer.getInt
      val nameBytes = new Array[Byte](nameSize)
      buffer.get(nameBytes)
      val name = new String(nameBytes, "UTF-8")
      SimpleCustomer(id, name)
    }
  }
}
