package com.kishan.kafkaConsumerObjects

import java.util

import com.kishan.mysql.MySqlStoreKafkaData._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

/*
* This singleton scala object consist of scala consumer code which will consume the message from
* kafka topic and stores messages into a table in MySql DB. Here the offset will be managed by a offset table
* in MySql instead of kafka offset management. When ever we receive a new messages each message will be processed
* and will be inserted into MySql table and offset of the partition will be updated for each message.
* Once Insert and Updates are done transaction will be committed. By doing this we can achieve Atomicity and
* exactly once processing kafka.
*
* This sample code also consist of approach to consume specific partitions of a kafka topic.
* */

object KafkaConsumerExternalDataStore {
  def main(args: Array[String]): Unit = {
    //Define Kafka Consumer Properties
    val topicName = "InboundTopic"
    var rCount = Int.MinValue
    val consumerProperties = Map[String, Object](
      "bootstrap.servers" -> "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "earliest",
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    ).asJava

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    val topicParition0: TopicPartition = new TopicPartition(topicName, 0)
    val topicParition1: TopicPartition = new TopicPartition(topicName, 1)
    val topicParition2: TopicPartition = new TopicPartition(topicName, 2)

    consumer.assign(util.Arrays.asList(topicParition0, topicParition1, topicParition2))

    println(s"Current offset position of p0 = ${consumer.position(topicParition0)}")
    println(s"Current offset position of p1 = ${consumer.position(topicParition1)}")
    println(s"Current offset position of p2 = ${consumer.position(topicParition2)}")

    consumer.seek(topicParition0, getOffsetFromDB(topicParition0))
    consumer.seek(topicParition1, getOffsetFromDB(topicParition1))
    consumer.seek(topicParition2, getOffsetFromDB(topicParition2))

    println(s"New offset position of p0 = ${consumer.position(topicParition0)}")
    println(s"New offset position of p1 = ${consumer.position(topicParition1)}")
    println(s"New offset position of p2 = ${consumer.position(topicParition2)}")

    println("Start Fetching Now..")
    try {
      do {
        val records = consumer.poll(1000).asScala
        rCount = records.size
        for (record <- records) {
          saveAndCommit(record)
        }
      } while (rCount != 0)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      consumer.close()
    }
  }
}
