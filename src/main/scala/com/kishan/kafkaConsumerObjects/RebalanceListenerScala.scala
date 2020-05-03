package com.kishan.kafkaConsumerObjects

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/*
* This Scala Class is used build Custom RebalanceListenerJava for Scala Consumer Application by
* extending ConsumerRebalanceListener which overrides the
* onPartitionsRevoked and onPartitionsAssigned methods.
* When ever Rebalance is triggered this methods will be invoked implicity by the leader
* For every message the offset information will be updated in currentOffset. Whenever the partition is revoked
* currentOffset will be committed
* */


class RebalanceListenerScala(consumer: KafkaConsumer[String, String]) extends ConsumerRebalanceListener {

  private var currentOffset: util.Map[TopicPartition, OffsetAndMetadata] = new util.HashMap()

  def addOffset(topicName: String, partition: Int, offset: Long): Unit = {
    currentOffset.put(new TopicPartition(topicName, partition), new OffsetAndMetadata(offset, "commit"))
  }

  def getCurrentOffset(): util.Map[TopicPartition, OffsetAndMetadata] = {
    return currentOffset
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    println("Partitions to be Revoked")
    for (partition <- partitions.asScala) {
      println(s"Partition: ${partition.partition()}")
    }

    println("Partitions to be Committed")
    for (parition <- currentOffset.keySet().asScala) {
      println(s"Partition: ${parition.partition()}")
    }

    consumer.commitSync(currentOffset)
    currentOffset.clear()
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    println("Partitions to be Assigned")
    for (partition <- partitions.asScala) {
      println(s"Partition: ${partition.partition()}")
    }
  }

}
