package com.kishan.kafkaConsumerObjects

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

/*
* This singleton object make use of RebalanceListener class and handles the rebalance activity when
* ever a consumer joins or leaves the group
* */

object SimpleKafkaConsumerWithRebalanceListener {
  def main(args: Array[String]): Unit = {

    val topicName = "InboundTopic"
    val bootStrapServers: String = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"
    val consumerGroupId = "InboundTopicGroup-4"
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

    //    val rebalanceListener = new RebalanceListenerJava(consumer)
    val rebalanceListener = new RebalanceListenerScala(consumer)

    consumer.subscribe(util.Arrays.asList(topicName), rebalanceListener)

    println("Started Consuming")
    try {
      do {
        val records = consumer.poll(Duration.ofSeconds(10)).asScala
//        recordCount = records.size
        for (record <- records) {
          println(s"Record Key: ${record.key()}, Record Value: ${record.value()}, Record Partition: ${record.partition()}")
          rebalanceListener.addOffset(record.topic(), record.partition(), record.offset())
        }
        consumer.commitSync(rebalanceListener.getCurrentOffset())
      } while (recordCount != 0)
    } catch {
      case e: Exception => {
        println(s"Caught Exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      consumer.commitSync(rebalanceListener.getCurrentOffset())
      consumer.close()
    }
  }
}
