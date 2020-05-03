use kafka_practice;
CREATE TABLE kafka_data_table(
kafka_key varchar(100),
kafka_timestamp bigint,
kafka_message_value varchar(1000),
PRIMARY KEY(kafka_key)
);
-- DROP TABLE kafka_data_table;
DESC kafka_data_table;
