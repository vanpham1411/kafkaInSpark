package org.example.customSerialize

import org.apache.kafka.common.serialization.Serializer
import org.codehaus.jackson.map.ObjectMapper

class UserSerializer extends Serializer[User]{
  override def serialize(s: String, t: User): Array[Byte] = {

    if(t==null)
      null
    else
    {
      val objectMapper = new ObjectMapper()
      objectMapper.writeValueAsString(t).getBytes
    }


  }
}
