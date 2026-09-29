-- Query layer, created on every server. Each Distributed table fans out to one
-- replica per shard; the empty database argument means "the shard's
-- default_database" from the cluster definition, which is what lets one
-- table name span shard_01, shard_02 and shard_03.
--
-- Sharding by customer keeps a customer's rows on one shard, so per-customer
-- lookups can be routed to a single shard and joins on customer_id can run
-- shard-locally.

CREATE DATABASE IF NOT EXISTS bank;

CREATE TABLE IF NOT EXISTS bank.customers          AS {any_db}.customers          ENGINE = Distributed(bank, '', customers, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.accounts           AS {any_db}.accounts           ENGINE = Distributed(bank, '', accounts, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.loans              AS {any_db}.loans              ENGINE = Distributed(bank, '', loans, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.transactions       AS {any_db}.transactions       ENGINE = Distributed(bank, '', transactions, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.loan_applications  AS {any_db}.loan_applications  ENGINE = Distributed(bank, '', loan_applications, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.clickstream        AS {any_db}.clickstream        ENGINE = Distributed(bank, '', clickstream, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.application_logs   AS {any_db}.application_logs   ENGINE = Distributed(bank, '', application_logs, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.call_center_events AS {any_db}.call_center_events ENGINE = Distributed(bank, '', call_center_events, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.daily_spend        AS {any_db}.daily_spend        ENGINE = Distributed(bank, '', daily_spend, cityHash64(customer_id));
CREATE TABLE IF NOT EXISTS bank.emi_daily          AS {any_db}.emi_daily          ENGINE = Distributed(bank, '', emi_daily);

-- Customer attributes for enrichment without a join. Reads through the
-- Distributed table, so every server sees the whole population.
CREATE DICTIONARY IF NOT EXISTS bank.customer_dict
(
    customer_id String,
    name        String,
    segment     String,
    city        String,
    risk_score  UInt16
)
PRIMARY KEY customer_id
SOURCE(CLICKHOUSE(QUERY 'SELECT customer_id, name, segment, city, risk_score FROM bank.customers FINAL'))
LIFETIME(MIN 60 MAX 120)
LAYOUT(COMPLEX_KEY_HASHED());
