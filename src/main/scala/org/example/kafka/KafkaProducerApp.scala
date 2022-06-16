package org.example.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)
    val topic = "text_topic"
    try {
//            //send with key
//            for (i <- 5 to 7) {
//              for(j <- 0 to 2) {
//                val record = new ProducerRecord[String, String](topic, j.toString, "word " + i+j)
//                val metadata = producer.send(record)
//                printf(s"sent record(key=%s value=%s) " +
//                  "meta(partition=%d, offset=%d)\n",
//                  record.key(), record.value(),
//                  metadata.get().partition(),
//                  metadata.get().offset())
//              }
//            }
      //send with key=null
      for (i <- 1 to 10) {
        val record = new ProducerRecord[String, String](topic, "word without key" + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }

}
