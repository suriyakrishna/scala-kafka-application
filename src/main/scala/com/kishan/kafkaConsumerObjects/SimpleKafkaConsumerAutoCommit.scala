package com.kishan.kafkaConsumerObjects

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

/*
* This Singleton object contains code for kafka consumer with auto commit.
* By default kafka enable.auto.commit=true and auto.commit.interval.ms=5sec
*/

object SimpleKafkaConsumerAutoCommit {
  def main(args: Array[String]): Unit = {

    val topicName = "InboundTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"
    val consumerGroupId = "InboundTopicGroup-1"
    var recordCount = Int.MinValue

    val consumerProperties = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> consumerGroupId
    ).asJava

    val consumer = new KafkaConsumer[String, String](consumerProperties)

    consumer.subscribe(util.Arrays.asList(topicName))

    try {
      do {
        val records = consumer.poll(Duration.ofSeconds(1)).asScala
        recordCount = records.size
        for (record <- records) {
          println(s"Partition: ${record.partition()}, Current Offset: ${record.offset()}, Record Value: ${record.value()}")
        }
      } while (recordCount != 0)
    } catch {
      case e: Exception => {
        println(s"Caught Exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      consumer.close()
    }
  }
}
