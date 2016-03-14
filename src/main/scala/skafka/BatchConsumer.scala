package skafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConversions._

class BatchConsumer(partitionsToConsume: Seq[TopicPartition], tenantMap: Map[String, Int], brokerConnection: String) {

  var finishedConsuming = false

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnection)
  props.put("group.id", "manual-management-batch")
  props.put("enable.auto.commit", "false")
  props.put("client.id", "test-batch-consumer")
  props.put("max.partition.fetch.bytes", 65536.toString) // maximum number of bytes to be fetched in a single poll, 65536 for roughly 200 messages
  props.put("auto.offset.reset", "earliest")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)

  lazy val BatchSize = 200

  consumer.assign(partitionsToConsume)

  def start() = {
    while (!finishedConsuming) {
      val records: ConsumerRecords[String, String] = consumer.poll(500)

      // process batch of records by partition
      records.partitions() foreach { partition: TopicPartition ⇒

        // grab records for a single partition
        val partitionRecords = records.records(partition)

        println(s"Fetched [${partitionRecords.size}] records with starting offset [${partitionRecords.head.offset()}]")

        // seem to lose ordering with the implicit conversion
        val recordBatch = partitionRecords.sortBy(_.offset()).take(BatchSize)

        // mapped tenant for the partition number
        val partitionTenant = tenantMap.map(_.swap).get(partition.partition())
        println(s"Consumed [${recordBatch.size}] messages for partition [$partitionTenant]")

        // always commit the next message to be consumed
        val offsetToCommit = recordBatch.last.offset() + 1L

        // commit offset for the partition
        consumer.commitSync(Map(partition -> new OffsetAndMetadata(offsetToCommit)))
        println(s"Committed offset [$offsetToCommit] for partition " +
          s"[$partitionTenant], [${partitionRecords.size - recordBatch.size}] records will be refetched")
      }

      if (!records.isEmpty) {
        println(s"Processed a batch of size [${records.count()}]")
      }
    }

    def stop() = {
      consumer.close()
      finishedConsuming = true
    }
  }

}
