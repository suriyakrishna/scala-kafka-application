package com.kishan.kafkaProducerObjects

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

/*
* This singleton scala object consists of sample code which will get input message from user and produce it to
* kafka topic and key will be having random number value. Scala readLine method is used to get input from user
* for each input producer record will be created and will be sent to the kafka topic. If user want to close the
* application user have to type exit.
*
* KafkaProducer - Send and forget
* */


object SimpleKafkaProducer {
  def main(args: Array[String]): Unit = {

    val topicName = "TestTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"

    val producerProperties = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    ).asJava

    val randomInt = scala.util.Random

    val kafkaProducer = new KafkaProducer[String, String](producerProperties)

    var userMessage = ""

    println("Type your message in the console. To close the console type exit.")
    try {
      while (userMessage.toLowerCase() != "exit") {
        userMessage = scala.io.StdIn.readLine("Enter your Message: ")
        if (userMessage.toLowerCase() != "exit") {
          val producerRecord = new ProducerRecord[String, String](topicName, math.abs(randomInt.nextLong()).toString, userMessage)
          kafkaProducer.send(producerRecord)
        }
      }
      println("User requested for exit. Closing producer application.")
    } catch {
      case e: Exception => {
        println(s"Caught Exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      kafkaProducer.close()
    }
  }
}
