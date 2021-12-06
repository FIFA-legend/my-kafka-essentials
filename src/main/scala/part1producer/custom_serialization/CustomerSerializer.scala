package part1producer.custom_serialization

import part1producer.custom_serialization.CustomSerialization.Customer
import org.apache.kafka.common.serialization.Serializer

import java.nio.ByteBuffer

class CustomerSerializer extends Serializer[Customer] {
  override def serialize(topic: String, data: Customer): Array[Byte] = {
    if (data == null) null
    else {
      val (serializedName, stringSize) =
        if (data.customerName != null) {
          val serializedName = data.customerName.getBytes("UTF-8")
          (serializedName, serializedName.length)
        } else {
          val serializedName = Array.empty[Byte]
          (serializedName, 0)
        }

      val buffer = ByteBuffer.allocate(4 + 4 + stringSize)
      buffer.putInt(data.customerId)
      buffer.putInt(stringSize)
      buffer.put(serializedName)
      buffer.array()
    }
  }
}
