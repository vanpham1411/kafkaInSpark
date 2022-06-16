package org.example.wordCount

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.io.Source

object Producer {
  def main(args: Array[String]) : Unit = {
    var props : Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")

    var producer = new KafkaProducer[String,String](props)

    val topic = "word-count"
    try {
      val filename = "./data/inputData.csv"
      for (line <- Source.fromFile(filename).getLines) {
        val record = new ProducerRecord[String, String](topic, line)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    }catch {
      case e : Exception => e.printStackTrace()

    }finally {
      producer.close()
    }
  }

}
