package com.kishan.kafkaProducerObjects

import java.io.File
import java.util.Scanner

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

/*
* This singleton scala object consists of code to produce messages in to kafka topic by reading a file.
* The sample file consists of two columns seperated by ':'. For each line the kafka producer record will be created and
* will be sent to kafka topic.
*
* KafkaProducer - Send and forget
* */


object FileToKafkaTopicKafkaProducer {
  def main(args: Array[String]): Unit = {

    val inputFilePath = "C:\\Users\\Kishan\\IdeaProjects\\kafkaToMySql\\sampleFiles\\sample_messages_50.csv"

    val topicName = "TestTopic"
    val bootStrapServers = "192.168.181.128:9092,192.168.181.128:9093,192.168.181.128:9094"

    val producerProperties: java.util.Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    ).asJava

    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProperties)

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
        kafkaProducer.send(producerRecord)
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

