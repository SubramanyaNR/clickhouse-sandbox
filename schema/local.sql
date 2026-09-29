-- Per-shard storage. bin/ch schema runs this once for every shard database a
-- server hosts, with {db} replaced by that database (shard_01, shard_02, ...).
-- The Keeper path is keyed by database, not by a server macro, because in the
-- circular layout one server holds replicas of two different shards.

CREATE DATABASE IF NOT EXISTS {db};

-- Entities change over time (balances, loan status). ReplacingMergeTree keeps
-- the latest version per key once parts merge; queries that cannot wait for a
-- merge use FINAL.
CREATE TABLE IF NOT EXISTS {db}.customers
(
    customer_id  String,
    name         String,
    segment      LowCardinality(String),
    city         LowCardinality(String),
    kyc_status   LowCardinality(String),
    risk_score   UInt16,
    onboarded_at DateTime64(3, 'UTC'),
    email        String,
    phone        String,
    event_time   DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{db}/customers', '{replica}', event_time)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS {db}.accounts
(
    account_id   String,
    customer_id  String,
    account_type LowCardinality(String),
    balance      Decimal(18, 2),
    opened_at    DateTime64(3, 'UTC'),
    status       LowCardinality(String),
    event_time   DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{db}/accounts', '{replica}', event_time)
ORDER BY (customer_id, account_id);

CREATE TABLE IF NOT EXISTS {db}.loans
(
    loan_id       String,
    customer_id   String,
    account_id    String,
    product       LowCardinality(String),
    principal     Decimal(18, 2),
    rate          Float32,
    tenure_months UInt16,
    emi_amount    Decimal(18, 2),
    outstanding   Decimal(18, 2),
    emi_day       UInt8,
    disbursed_at  DateTime64(3, 'UTC'),
    status        LowCardinality(String),
    dpd_bucket    LowCardinality(String),
    event_time    DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{db}/loans', '{replica}', event_time)
ORDER BY (customer_id, loan_id);

-- Ordered by customer first: the product questions are "what did this
-- customer do", and the projection below covers the time-range rollups.
CREATE TABLE IF NOT EXISTS {db}.transactions
(
    txn_id            String,
    account_id        String,
    customer_id       String,
    loan_id           Nullable(String),
    amount            Decimal(18, 2),
    currency          LowCardinality(String),
    txn_type          LowCardinality(String),
    merchant_category LowCardinality(String),
    channel           LowCardinality(String),
    status            LowCardinality(String),
    failure_reason    LowCardinality(Nullable(String)),
    event_time        DateTime64(3, 'UTC'),
    event_date        Date,
    PROJECTION by_day_type
    (
        SELECT event_date, txn_type, status, count(), sum(amount)
        GROUP BY event_date, txn_type, status
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/transactions', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (customer_id, event_time);

CREATE TABLE IF NOT EXISTS {db}.loan_applications
(
    application_id        String,
    customer_id           String,
    product               LowCardinality(String),
    requested_amount      Decimal(18, 2),
    stage                 LowCardinality(String),
    stage_seq             UInt8,
    decision              LowCardinality(String),
    rejection_reason_code LowCardinality(Nullable(String)),
    rejection_reason_text Nullable(String),
    risk_score_at_apply   UInt16,
    stage_entered_at      DateTime64(3, 'UTC'),
    event_time            DateTime64(3, 'UTC'),
    event_date            Date
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/loan_applications', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (customer_id, application_id, stage_seq);

CREATE TABLE IF NOT EXISTS {db}.clickstream
(
    event_id    String,
    session_id  String,
    customer_id String,
    screen      LowCardinality(String),
    action      LowCardinality(String),
    funnel_step Int8,
    device      LowCardinality(String),
    app_version LowCardinality(String),
    event_time  DateTime64(3, 'UTC'),
    event_date  Date
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/clickstream', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, app_version, device, session_id, event_time);

CREATE TABLE IF NOT EXISTS {db}.application_logs
(
    log_id      String,
    customer_id String,
    service     LowCardinality(String),
    module      LowCardinality(String),
    level       LowCardinality(String),
    error_code  LowCardinality(Nullable(String)),
    message     String,
    app_version LowCardinality(String),
    device      LowCardinality(String),
    is_crash    Bool,
    event_time  DateTime64(3, 'UTC'),
    event_date  Date
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/application_logs', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, app_version, device, module, event_time)
TTL toDate(event_date) + INTERVAL 1 YEAR;

CREATE TABLE IF NOT EXISTS {db}.call_center_events
(
    ticket_id       String,
    customer_id     String,
    loan_id         Nullable(String),
    intent          LowCardinality(String),
    category        LowCardinality(String),
    channel         LowCardinality(String),
    status          LowCardinality(String),
    resolution      LowCardinality(String),
    csat            Nullable(UInt8),
    handle_time_min UInt16,
    opened_at       DateTime64(3, 'UTC'),
    closed_at       Nullable(DateTime64(3, 'UTC')),
    event_time      DateTime64(3, 'UTC'),
    event_date      Date
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/call_center_events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (customer_id, opened_at, ticket_id);

-- Daily spend per customer and category, maintained on insert. Sums merge
-- correctly across parts, so no FINAL is needed to read it.
CREATE TABLE IF NOT EXISTS {db}.daily_spend
(
    event_date        Date,
    customer_id       String,
    merchant_category LowCardinality(String),
    txn_count         UInt64,
    amount            Decimal(38, 2)
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{db}/daily_spend', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, customer_id, merchant_category);

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.daily_spend_mv TO {db}.daily_spend AS
SELECT event_date, customer_id, merchant_category, count() AS txn_count, sum(amount) AS amount
FROM {db}.transactions
WHERE status = 'success' AND txn_type != 'emi_payment'
GROUP BY event_date, customer_id, merchant_category;

-- EMI outcomes per day, as uniqueness-safe aggregate states.
CREATE TABLE IF NOT EXISTS {db}.emi_daily
(
    event_date Date,
    attempts   AggregateFunction(count),
    failures   AggregateFunction(countIf, UInt8),
    customers  AggregateFunction(uniq, String)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{db}/emi_daily', '{replica}')
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.emi_daily_mv TO {db}.emi_daily AS
SELECT
    event_date,
    countState() AS attempts,
    countIfState(status != 'success') AS failures,
    uniqState(customer_id) AS customers
FROM {db}.transactions
WHERE txn_type = 'emi_payment'
GROUP BY event_date;
