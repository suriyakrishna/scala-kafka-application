package com.kishan.kafkaConsumerObjects;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListenerJava implements ConsumerRebalanceListener {

    private KafkaConsumer<String, String> consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap();

    public RebalanceListenerJava(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public void addOffset(String topicName, int partition, long offset) {
        currentOffset.put(new TopicPartition(topicName, partition), new OffsetAndMetadata(offset, "commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffset() {
        return currentOffset;
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions are Assigned");
        for (TopicPartition partition : partitions) {
            System.out.println("Partitions: " + partition);
        }
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions to be Revoked");
        for (TopicPartition partition : partitions) {
            System.out.println("Partitions: " + partition.partition());
        }
        System.out.println("Following Partitions to be committed");
        for (TopicPartition partition : currentOffset.keySet()) {
            System.out.println("Partitions: " + partition.partition());
        }

        consumer.commitSync(currentOffset);
        currentOffset.clear();
    }
}
