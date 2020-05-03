package com.kishan.kafkaConsumerObjects

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

/*
* This singleton object consist of code for Kafka consumer manual offset commit using CommitAsync Method
* Here we have used CommitAsync to commit for every poll and at finally block we have used CommitSync()
* CommitSync() method will retry for recoverable errors but commitAsync will not retry
*/
object SimpleKafkaConsumerCommitAsync {
  def main(args: Array[String]): Unit = {

    val topicName = "InboundTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"
    val consumerGroupId = "InboundTopicGroup-2"
    var recordCount = Int.MinValue

    val consumerProperties = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> consumerGroupId,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ).asJava

    val consumer = new KafkaConsumer[String, String](consumerProperties)

    consumer.subscribe(util.Arrays.asList(topicName))

    try {
      do {
        val records = consumer.poll(Duration.ofSeconds(10)).asScala
        recordCount = records.size
        for (record <- records) {
          println(s"Partition: ${record.partition()}, Current Offset: ${record.offset()}, Record Value: ${record.value()}")
        }
        consumer.commitAsync()
      } while (recordCount != 0)
    } catch {
      case e: Exception => {
        println(s"Caught Exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      consumer.commitSync()
      consumer.close()
    }
  }
}
