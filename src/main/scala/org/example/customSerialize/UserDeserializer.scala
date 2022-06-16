package org.example.customSerialize

import org.apache.kafka.common.serialization.Deserializer
import org.codehaus.jackson.map.ObjectMapper

class UserDeserializer extends Deserializer[User]{
  override def deserialize(s: String, bytes: Array[Byte]): User = {
    val mapper = new ObjectMapper()
    val user = mapper.readValue(bytes, classOf[User])
    user
  }
}
