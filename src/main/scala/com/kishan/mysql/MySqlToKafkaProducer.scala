package com.kishan.mysql

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.io.Source

/*
* This scala object consist of method to get the data from the MySql table and create JSON for each record.
* The JSON will be sent to kafka topic.
* The key for message will be concatenated value of the list of keys provided as input.
* */

object MySqlToKafkaProducer {

  def sendMessageAsJson(producer: KafkaProducer[String, String], topicName: String, sqlQuery: String, recordKeys: List[String]): Unit = {
    //Reading Properties File
    val propertiesFile = Source.fromResource("mysql.properties").getLines().map(a => a.split("=")).map(a => (a(0), a(1))).toMap.asJava

    val dbProperties = new Properties()
    dbProperties.putAll(propertiesFile)

    //Create JDBC Connection
    val connectionURL: String = dbProperties.get("com.kishan.db.mysql.jdbc_url").toString
    val userName: String = dbProperties.get("com.kishan.db.mysql.username").toString
    val password: String = dbProperties.get("com.kishan.db.mysql.password").toString

    val conn: Connection = DriverManager.getConnection(connectionURL, userName, password)

    val createStatement = conn.createStatement()
    val resultset = createStatement.executeQuery(sqlQuery)
    val resultMetadata = resultset.getMetaData
    var columns: List[String] = List()
    println("Table Information")
    for (i <- 1 to resultMetadata.getColumnCount) {
      println(s"Column Index: ${i}, Column Name: ${resultMetadata.getColumnName(i)}, Column Type: ${resultMetadata.getColumnTypeName(i)}")
      columns = columns :+ resultMetadata.getColumnName(i)
    }

    // Creating JSON and Sending record in to kafka topic
    while (resultset.next()) {
      var json: JSONObject = new JSONObject()
      var key: String = ""
      for (column <- columns) {
        val columnValue = resultset.getObject(column)
        if (recordKeys.contains(column.toLowerCase)) {
          key = key + columnValue.toString
        }
        json.put(column, columnValue)
      }
      val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, json.toString())
      producer.send(producerRecord)
    }
    conn.close()
  }
}
