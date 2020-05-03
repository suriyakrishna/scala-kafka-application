package com.kishan.kafkaProducerObjects

import com.kishan.mysql.MySqlToKafkaProducer.sendMessageAsJson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

/*
* This singleton scala object will make use of method defined in the com.kishan.mysql.MySqlToKafkaProducer and
* send message into kafka topic as JSON
* */

object MySqlToKafkaTopic {
  def main(args: Array[String]): Unit = {

    val topicName = "TestTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"

    val producerProperties: java.util.Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    ).asJava

    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProperties)
    val keysList: List[String] = "id,countrycode".split(",").map(a => a.trim).toList
    val sqlQuery = "select * from world.city where lower(countrycode)='ind'"
    println(s"Producing message to topic: ${topicName}")
    try {
      sendMessageAsJson(kafkaProducer, topicName, sqlQuery, keysList)
      println("All message sent to topic successfully.")
    } catch {
      case e: Exception => {
        println(s"Caught exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      kafkaProducer.close()
    }
  }
}
