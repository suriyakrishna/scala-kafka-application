USE kafka_practice;
SHOW TABLES;
DESCRIBE offset_table;
INSERT INTO offset_table(topic_name, partition_value, offset_value) VALUES("InboundTopic", 0, 0);
INSERT INTO offset_table(topic_name, partition_value, offset_value) VALUES("InboundTopic", 1, 0);
INSERT INTO offset_table(topic_name, partition_value, offset_value) VALUES("InboundTopic", 2, 0);
SELECT * FROM offset_table;