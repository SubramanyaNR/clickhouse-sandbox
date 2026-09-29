-- Continuous ingest from kafka-sandbox. Needs both stacks started in linked
-- mode (make linked in each), then: make kafka
--
-- Created on every server with the same consumer group, so Kafka spreads the
-- topic's partitions across the three of them. Each server's view writes
-- through the Distributed table, which re-shards rows by customer.
--
-- Timestamps arrive as ISO 8601 strings with an offset and are parsed in the
-- view rather than by the Kafka engine, so the table does not depend on
-- date_time_input_format being set for background consumers.

CREATE TABLE IF NOT EXISTS bank.transactions_kafka
(
    txn_id            String,
    account_id        String,
    customer_id       String,
    loan_id           Nullable(String),
    amount            Decimal(18, 2),
    currency          String,
    txn_type          String,
    merchant_category String,
    channel           String,
    status            String,
    failure_reason    Nullable(String),
    event_time        String,
    event_date        Date
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'ks-kafka1:9092,ks-kafka2:9092,ks-kafka3:9092',
    kafka_topic_list = 'bank.dbo.transactions',
    kafka_group_name = 'clickhouse-bank-transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

CREATE MATERIALIZED VIEW IF NOT EXISTS bank.transactions_kafka_mv TO bank.transactions AS
SELECT
    txn_id, account_id, customer_id, loan_id, amount, currency, txn_type,
    merchant_category, channel, status, failure_reason,
    parseDateTime64BestEffort(event_time, 3, 'UTC') AS event_time,
    event_date
FROM bank.transactions_kafka;
