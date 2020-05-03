package com.kishan.kafkaProducerObjects

import java.io.File
import java.util.Scanner

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

/*
* This singleton scala object consist of Simple kafkaProducer which will get the acknowledgement for each message we send.
* If we call get method on producer.send() we will get the acknowledgement for the message we send. This will cause delay in sending the next
* message. But we can guarantee that every message we send will reach the topic.
* */

object KafkaProducerWithAcknowlegement {
  def main(args: Array[String]): Unit = {

    val inputFilePath = "C:\\Users\\Kishan\\IdeaProjects\\kafkaToMySql\\sampleFiles\\sample_messages_50.csv"

    val topicName = "TestTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"

    val producerProperties = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    ).asJava

    val randomInt = scala.util.Random

    val kafkaProducer = new KafkaProducer[String, String](producerProperties)

    //Read input File
    val file: File = new File(inputFilePath)

    //check file exists
    if (file.exists()) {
      println("File exists")
    } else {
      println("File doesn't exists. Check input path. Exiting...")
      System.exit(1)
    }

    val scanner: Scanner = new Scanner(file)

    var numRecordsSent: Int = 0
    println(s"Producing message to the topic: ${topicName}")
    try {
      while (scanner.hasNextLine) {
        // The sample file is csv file delimited by ':' the first column value is the key and second column value is the message
        val record = scanner.nextLine().split(":")
        val producerRecord = new ProducerRecord[String, String](topicName, record(0), record(1))
        val ack = kafkaProducer.send(producerRecord).get()
        println(s"Message Partition: ${ack.partition()}, Message Offset: ${ack.offset()}")
        numRecordsSent += 1
      }
      println(s"Total number of records sent: ${numRecordsSent}")
      println("All messages have been sent to the topic successfully.")
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
