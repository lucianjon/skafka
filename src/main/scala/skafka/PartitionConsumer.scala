package skafka

import java.util.Properties

import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata

class PartitionConsumer(
    partitionsToConsume: Seq[TopicPartition],
    tenantMap: Map[String, Int],
    brokerConnection: String,
    seekToOffset: Option[Long]
  ) {

  var finishedConsuming = false

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnection)
  props.put("group.id", "manual-management")
  props.put("enable.auto.commit", "false")
  props.put("client.id", "test-partition-consumer")
  props.put("max.partition.fetch.bytes", 65536.toString)
  props.put("auto.offset.reset", "earliest")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.assign(partitionsToConsume)

  seekToOffset foreach { offset =>
    partitionsToConsume foreach { partition ⇒
      consumer.seek(partition, offset)
    }
  }

  def start() = {
    while (!finishedConsuming) {
      val records: ConsumerRecords[String, String] = consumer.poll(500)

      // process batch of records by partition
      records.partitions() foreach { partition: TopicPartition ⇒

        // grab records for a single partition
        val partitionRecords = records.records(partition)

        // mapped tenant for the partition number
        val partitionTenant = tenantMap.map(_.swap).get(partition.partition())
        println(s"Consumed [${partitionRecords.size}] messages for partition [$partitionTenant]")

        // always commit the next message to be consumed
        val offsetToCommit = partitionRecords.last.offset() + 1L

        // commit offset for the partition
        consumer.commitSync(Map(partition -> new OffsetAndMetadata(offsetToCommit)))
        println(s"Committed offset [$offsetToCommit] for partition [$partitionTenant]")
      }

      if (!records.isEmpty) {
        println(s"Processed a batch of size [${records.count()}]")
      }
    }
  }

  def stop() = {
    consumer.close()
    finishedConsuming = true
  }

}
