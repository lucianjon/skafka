package skafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._

class SimpleConsumer(brokerConnection: String, topics: Seq[String]) {

  var finishedConsuming = false

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnection)
  props.put("group.id", "auto-management")
  props.put("enable.auto.commit", "true")
  props.put("client.id", "test-simple-consumer")
  props.put("max.partition.fetch.bytes", 1024.toString)
  props.put("auto.offset.reset", "earliest")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)

  val FetchTimeoutMillis = 500

  consumer.subscribe(topics)

  def start() = {
    while (!finishedConsuming) {
      val records: ConsumerRecords[String, String] = consumer.poll(FetchTimeoutMillis)
      if (!records.isEmpty) {
        println(s"Consumed a batch of size [${records.count()}] across [${records.partitions().size}] partitions")
      }
    }
  }

  def stop() = {
    consumer.close()
    finishedConsuming = true
  }
}
