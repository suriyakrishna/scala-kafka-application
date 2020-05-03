CREATE DATABASE kafka_practice;
USE kafka_practice;
CREATE TABLE offset_table(
topic_name VARCHAR(70),
partition_value INT,
offset_value LONG,
PRIMARY KEY(topic_name, partition_value)
)


