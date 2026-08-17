-- ============================================================
-- Financial Platform — PostgreSQL Warehouse Schema
-- ============================================================

-- ── Dimension: Time ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_time (
    time_id     BIGSERIAL PRIMARY KEY,
    event_time  TIMESTAMPTZ NOT NULL,
    year        SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL,
    day_of_week SMALLINT    NOT NULL,
    day_of_year SMALLINT    NOT NULL,
    hour        SMALLINT    NOT NULL,
    minute      SMALLINT    NOT NULL,
    UNIQUE (event_time)
);

-- ── Dimension: Company / Instrument ─────────────────────────
CREATE TABLE IF NOT EXISTS dim_company (
    company_id  BIGSERIAL   PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL,
    currency    VARCHAR(10) NOT NULL DEFAULT 'USD',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol)
);

-- ── Dimension: Market ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_market (
    market_id   BIGSERIAL   PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    asset_class VARCHAR(20) NOT NULL,   -- STOCK | CRYPTO | FOREX
    region      VARCHAR(50),
    UNIQUE (name, asset_class)
);

-- ── Fact: Stock Prices ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_stock_price (
    id          BIGSERIAL    PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    price       NUMERIC(18,6) NOT NULL,
    open_price  NUMERIC(18,6),
    high_price  NUMERIC(18,6),
    low_price   NUMERIC(18,6),
    close_price NUMERIC(18,6),
    volume      NUMERIC(24,4),
    ma5         NUMERIC(18,6),
    ma20        NUMERIC(18,6),
    ma50        NUMERIC(18,6),
    rsi         NUMERIC(8,4),
    currency    VARCHAR(10)  NOT NULL DEFAULT 'USD',
    event_time  TIMESTAMPTZ  NOT NULL,
    ingested_at TIMESTAMPTZ,
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_symbol_time ON fact_stock_price (symbol, event_time DESC);

-- ── Fact: Crypto Prices ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_crypto (
    id           BIGSERIAL    PRIMARY KEY,
    symbol       VARCHAR(20)  NOT NULL,
    price        NUMERIC(24,8) NOT NULL,
    high_24h     NUMERIC(24,8),
    low_24h      NUMERIC(24,8),
    volume_24h   NUMERIC(30,4),
    quote_volume NUMERIC(30,4),
    ma5          NUMERIC(24,8),
    ma20         NUMERIC(24,8),
    ma50         NUMERIC(24,8),
    rsi          NUMERIC(8,4),
    event_time   TIMESTAMPTZ  NOT NULL,
    ingested_at  TIMESTAMPTZ,
    loaded_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crypto_symbol_time ON fact_crypto (symbol, event_time DESC);

-- ── Fact: Forex Rates ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_forex (
    id              BIGSERIAL    PRIMARY KEY,
    base_currency   VARCHAR(10)  NOT NULL,
    target_currency VARCHAR(10)  NOT NULL,
    rate            NUMERIC(18,8) NOT NULL,
    pair            VARCHAR(20)  GENERATED ALWAYS AS (base_currency || '_' || target_currency) STORED,
    event_time      TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_forex_pair_time ON fact_forex (base_currency, target_currency, event_time DESC);

-- ── Seed dim_market ──────────────────────────────────────────
INSERT INTO dim_market (name, asset_class, region)
VALUES
    ('NASDAQ',  'STOCK',  'US'),
    ('NYSE',    'STOCK',  'US'),
    ('Binance', 'CRYPTO', 'Global'),
    ('Forex',   'FOREX',  'Global')
ON CONFLICT (name, asset_class) DO NOTHING;

-- ============================================================
-- Analytical Views (used by Superset datasets)
-- ============================================================

-- Latest price per stock symbol
CREATE OR REPLACE VIEW vw_stock_live AS
SELECT DISTINCT ON (symbol)
    symbol, price, open_price, high_price, low_price, close_price,
    volume, currency, ma5, ma20, ma50, rsi, event_time
FROM fact_stock_price
ORDER BY symbol, event_time DESC;

-- Latest price per crypto symbol
CREATE OR REPLACE VIEW vw_crypto_live AS
SELECT DISTINCT ON (symbol)
    symbol, price, high_24h, low_24h, volume_24h, quote_volume,
    ma5, ma20, ma50, rsi, event_time
FROM fact_crypto
ORDER BY symbol, event_time DESC;

-- Latest forex rates
CREATE OR REPLACE VIEW vw_forex_live AS
SELECT DISTINCT ON (base_currency, target_currency)
    base_currency, target_currency, rate, pair, event_time
FROM fact_forex
ORDER BY base_currency, target_currency, event_time DESC;

-- Top gainers & losers: % change between oldest and newest price in last 24 h
CREATE OR REPLACE VIEW vw_stock_performance AS
WITH bounds AS (
    SELECT
        symbol,
        MIN(price) FILTER (WHERE event_time = (SELECT MIN(event_time) FROM fact_stock_price s2 WHERE s2.symbol = s.symbol AND s2.event_time >= NOW() - INTERVAL '24 hours')) AS open_24h,
        MAX(event_time) AS latest_time
    FROM fact_stock_price s
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY symbol
),
latest AS (
    SELECT DISTINCT ON (f.symbol) f.symbol, f.price AS close_24h
    FROM fact_stock_price f
    JOIN bounds b ON f.symbol = b.symbol
    ORDER BY f.symbol, f.event_time DESC
)
SELECT
    l.symbol,
    b.open_24h,
    l.close_24h,
    ROUND(((l.close_24h - b.open_24h) / NULLIF(b.open_24h, 0)) * 100, 2) AS pct_change
FROM latest l JOIN bounds b ON l.symbol = b.symbol;

-- Hourly volume aggregation for stocks
CREATE OR REPLACE VIEW vw_stock_volume_hourly AS
SELECT
    symbol,
    DATE_TRUNC('hour', event_time) AS hour_bucket,
    SUM(volume) AS total_volume,
    COUNT(*) AS tick_count
FROM fact_stock_price
GROUP BY symbol, DATE_TRUNC('hour', event_time);

-- Daily volume aggregation for stocks
CREATE OR REPLACE VIEW vw_stock_volume_daily AS
SELECT
    symbol,
    DATE_TRUNC('day', event_time) AS day_bucket,
    SUM(volume) AS total_volume,
    AVG(price)  AS avg_price,
    MAX(high_price) AS daily_high,
    MIN(low_price)  AS daily_low,
    COUNT(*) AS tick_count
FROM fact_stock_price
GROUP BY symbol, DATE_TRUNC('day', event_time);

-- Correlation helper: aligned BTC and ETH prices by 5-min bucket
CREATE OR REPLACE VIEW vw_correlation_btc_eth AS
SELECT
    DATE_TRUNC('minute', b.event_time) - 
        (EXTRACT(minute FROM b.event_time)::int % 5 * INTERVAL '1 minute') AS bucket,
    AVG(b.price) AS btc_price,
    AVG(e.price) AS eth_price
FROM fact_crypto b
JOIN fact_crypto e
    ON DATE_TRUNC('minute', e.event_time) - 
        (EXTRACT(minute FROM e.event_time)::int % 5 * INTERVAL '1 minute')
     = DATE_TRUNC('minute', b.event_time) - 
        (EXTRACT(minute FROM b.event_time)::int % 5 * INTERVAL '1 minute')
WHERE b.symbol = 'BTCUSDT' AND e.symbol = 'ETHUSDT'
GROUP BY 1;

-- Correlation helper: NASDAQ (MSFT proxy) vs BTC
CREATE OR REPLACE VIEW vw_correlation_nasdaq_btc AS
SELECT
    DATE_TRUNC('hour', s.event_time) AS bucket,
    AVG(s.price) AS nasdaq_price,
    AVG(c.price) AS btc_price
FROM fact_stock_price s
JOIN fact_crypto c
    ON DATE_TRUNC('hour', c.event_time) = DATE_TRUNC('hour', s.event_time)
WHERE s.symbol = 'MSFT' AND c.symbol = 'BTCUSDT'
GROUP BY 1;

-- Moving averages time series (last 200 ticks per symbol)
CREATE OR REPLACE VIEW vw_moving_averages AS
SELECT symbol, price, ma5, ma20, ma50, rsi, event_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_time DESC) AS rn
    FROM fact_stock_price
    WHERE ma5 IS NOT NULL
) sub
WHERE rn <= 200;

-- Candlestick OHLC aggregated per 5-minute candle
CREATE OR REPLACE VIEW vw_ohlc_stock AS
SELECT
    symbol,
    DATE_TRUNC('minute', event_time) - 
        (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') AS candle_time,
    FIRST_VALUE(open_price)  OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') ORDER BY event_time)      AS open,
    MAX(high_price)          OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                           AS high,
    MIN(low_price)           OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                           AS low,
    LAST_VALUE(close_price)  OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
    SUM(volume)              OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                           AS volume
FROM fact_stock_price;

CREATE OR REPLACE VIEW vw_ohlc_crypto AS
SELECT
    symbol,
    DATE_TRUNC('minute', event_time) - 
        (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') AS candle_time,
    FIRST_VALUE(price) OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') ORDER BY event_time)                      AS open,
    MAX(high_24h)      OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                                          AS high,
    MIN(low_24h)       OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                                          AS low,
    LAST_VALUE(price)  OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute') ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
    SUM(volume_24h)    OVER (PARTITION BY symbol, DATE_TRUNC('minute', event_time) - (EXTRACT(minute FROM event_time)::int % 5 * INTERVAL '1 minute'))                                          AS volume
FROM fact_crypto;

-- ============================================================
-- ML Prediction Tables & Views
-- ============================================================

-- Stores one row per model-run per symbol
CREATE TABLE IF NOT EXISTS ml_stock_predictions (
    id              BIGSERIAL     PRIMARY KEY,
    symbol          VARCHAR(20)   NOT NULL,
    prediction      VARCHAR(10)   NOT NULL,   -- RISE | DROP
    confidence      NUMERIC(10,8) NOT NULL,   -- mean P(RISE) across all models
    rf_probability  NUMERIC(10,8),
    gbt_probability NUMERIC(10,8),
    lr_probability  NUMERIC(10,8),
    model_votes     SMALLINT      NOT NULL,   -- 0–3 models voting RISE
    current_price   NUMERIC(18,6),
    predicted_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ml_pred_symbol_time
    ON ml_stock_predictions (symbol, predicted_at DESC);

-- Latest prediction per symbol ranked by confidence
-- TOP_RISE: top 5 by confidence (highest P(RISE))
-- TOP_DROP: top 5 by inverse confidence (lowest P(RISE) = highest P(DROP))
CREATE OR REPLACE VIEW vw_ml_top_movers AS
WITH latest AS (
    SELECT DISTINCT ON (symbol)
        symbol, prediction, confidence,
        rf_probability, gbt_probability, lr_probability,
        model_votes, current_price, predicted_at
    FROM ml_stock_predictions
    ORDER BY symbol, predicted_at DESC
),
rise_ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY confidence DESC) AS signal_rank,
           'TOP_RISE'::TEXT AS category
    FROM latest
),
drop_ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY confidence ASC) AS signal_rank,
           'TOP_DROP'::TEXT AS category
    FROM latest
)
SELECT symbol, prediction, confidence, rf_probability, gbt_probability,
       lr_probability, model_votes, current_price, predicted_at,
       signal_rank, category
FROM rise_ranked WHERE signal_rank <= 5
UNION ALL
SELECT symbol, prediction, confidence, rf_probability, gbt_probability,
       lr_probability, model_votes, current_price, predicted_at,
       signal_rank, category
FROM drop_ranked WHERE signal_rank <= 5;

-- Latest prediction per symbol with human-readable agreement level
CREATE OR REPLACE VIEW vw_ml_model_agreement AS
SELECT DISTINCT ON (symbol)
    symbol, prediction, confidence, model_votes, current_price, predicted_at,
    CASE model_votes
        WHEN 3 THEN 'UNANIMOUS_RISE'
        WHEN 2 THEN 'MAJORITY_RISE'
        WHEN 1 THEN 'MAJORITY_DROP'
        WHEN 0 THEN 'UNANIMOUS_DROP'
    END AS agreement_level
FROM ml_stock_predictions
ORDER BY symbol, predicted_at DESC;
