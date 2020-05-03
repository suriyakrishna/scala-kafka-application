package com.kishan.kafkaConsumerObjects

import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

/*
* This Singleton Object contains sample code to consume a kafka topic and
* print the message in the console without committing offsets
*/

object SimpleKafkaConsumer {
  def main(args: Array[String]): Unit = {

    val topicName = "InboundTopic"
    val bootStrapServers: String = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"
    val consumerGroupId = "InboundTopicGroup-0"
    var recordCount = Int.MinValue

    val consumerProperties = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "earliest",
      "group.id" -> consumerGroupId
    ).asJava

    val consumer = new KafkaConsumer[String, String](consumerProperties)

    consumer.subscribe(util.Arrays.asList(topicName))
    println("Started Consuming")
    try {
      do {
        val records = consumer.poll(1000).asScala
        recordCount = records.size
        for (record <- records) {
          println(s"Record Key: ${record.key()}, Record Value: ${record.value()}")
        }
      } while (recordCount != 0)
      println("No Records to consume")
    } catch {
      case e: Exception => {
        println(s"Caught Exception. ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      consumer.close()
    }
  }
}
