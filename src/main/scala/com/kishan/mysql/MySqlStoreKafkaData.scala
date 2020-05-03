package com.kishan.mysql

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.io.Source

object MySqlStoreKafkaData {

  //This method will get the input record and insert the record in the MySql DB Table. Also, it will update the committed offset in the Offset table and then commit will be executed.
  def saveAndCommit(consumerRecord: ConsumerRecord[String, String]): Unit = {

    val propertiesFile = Source.fromResource("mysql.properties").getLines().map(a => a.split("=")).map(a => (a(0), a(1))).toMap.asJava
    val dbProperties = new Properties()
    dbProperties.putAll(propertiesFile)

    //Create JDBC Connection and setting auto commit to false
    val connectionURL: String = dbProperties.get("com.kishan.db.mysql.jdbc_url").toString
    val userName: String = dbProperties.get("com.kishan.db.mysql.username").toString
    val password: String = dbProperties.get("com.kishan.db.mysql.password").toString

    val conn: Connection = DriverManager.getConnection(connectionURL, userName, password)
    conn.setAutoCommit(false)

    //Insert Statement for inserting the kafka message into kafka_practice.kafka_data_table
    val insertStatement = "INSERT INTO kafka_practice.kafka_data_table (kafka_key, kafka_timestamp, kafka_message_value) VALUES(?,?,?)"
    val insertRecord = conn.prepareStatement(insertStatement)
    insertRecord.setString(1, consumerRecord.key())
    insertRecord.setLong(2, consumerRecord.timestamp())
    insertRecord.setString(3, consumerRecord.value())

    //Update Statement for updating the offset in the offset table
    val commitUpdateStatement = "UPDATE kafka_practice.offset_table SET offset_value=? WHERE topic_name=? AND partition_value=?"
    val updateCommit = conn.prepareStatement(commitUpdateStatement)
    updateCommit.setLong(1, consumerRecord.offset() + 1)
    updateCommit.setString(2, consumerRecord.topic())
    updateCommit.setInt(3, consumerRecord.partition())

    //Statement Execution
    insertRecord.executeUpdate()
    updateCommit.executeUpdate()

    //Commit and Closing the connection
    conn.commit()
    conn.close()
  }

  //Method to get the committed offset from MySql DB Table
  def getOffsetFromDB(topicPartition: TopicPartition): Long = {
    var offset = 0

    val propertiesFile = Source.fromResource("mysql.properties").getLines().map(a => a.split("=")).map(a => (a(0), a(1))).toMap.asJava
    val dbProperties = new Properties()
    dbProperties.putAll(propertiesFile)

    //Create JDBC Connection and setting auto commit to false
    val connectionURL: String = dbProperties.get("com.kishan.db.mysql.jdbc_url").toString
    val userName: String = dbProperties.get("com.kishan.db.mysql.username").toString
    val password: String = dbProperties.get("com.kishan.db.mysql.password").toString
    val conn: Connection = DriverManager.getConnection(connectionURL, userName, password)
    val sql: String = s"SELECT offset_value FROM kafka_practice.offset_table WHERE topic_name='${topicPartition.topic()}' AND partition_value=${topicPartition.partition()}"
    val execStmt = conn.createStatement()
    val rs: ResultSet = execStmt.executeQuery(sql)
    if (rs.next()) {
      offset = rs.getInt("offset_value")
    }
    conn.close()
    return offset
  }

}
