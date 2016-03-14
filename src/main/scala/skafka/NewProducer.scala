package skafka

import java.util.Properties
import java.util.concurrent.{Future => JFuture}

import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future
import scala.util.Random

class NewProducer(brokerConnection: String) {

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnection)
  props.put("acks", "all")
  props.put("max.block.ms", 10000.toString) // will block on fetching metadata or full buffer
  props.put("retries", 0.toString)
  props.put("batch.size", 16384.toString)
  props.put("linger.ms", 1.toString)
  props.put("client.id", "test-producer")
  props.put("buffer.memory", 33554432.toString) // 35MB
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  println(s"Producer started with props [${props.toString}]")

  val tenants = Seq(
    "tenant0",
    "tenant1",
    "tenant2",
    "tenant3",
    "tenant4"
  )

  val tenantsMap: Map[String, Int] = {
    tenants.zipWithIndex map { case (tenant, idx) =>
      tenant -> idx
    }
  }.toMap

  private val random = new Random

  def send(topic: String): Future[RecordMetadata] = Future {

    val key = tenants(random.nextInt(tenants.size))
    val partitionNumber = tenantsMap(key)
    val value = s"Some data for $key"

    val record = new ProducerRecord[String, String](topic, partitionNumber, key, value)

    val rec: JFuture[RecordMetadata] = producer.send(record)

    rec.get
  }
}
