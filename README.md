# Financial Market Data Platform

A fully containerised end-to-end data pipeline that ingests real-time stock, crypto, and forex market data, processes it with Apache Spark, stores it in a Delta Lake and a PostgreSQL warehouse, predicts uptrend and downtrend stocks with SparkML and visualises it through Apache Superset and OpenSearch Dashboards.

---

## Architecture

```
            Python APIs Crawler
         +---------------------+
         | Stock               |
         | Crypto              |
         | Forex               |
         +----------+----------+
                    |
              Airflow DAG
                    |
          Ingestion (Python)
                    |
            Kafka (Streaming)
                    |
          +---------+---------+
          |                   |
      Raw Data           Real-time Stream
          |                   |
        MinIO         Spark Streaming
          |                   |
          +---------+----------+
                    |
              Data Cleaning
                    |
            Feature Engineering
                    |
              Delta Lake
                    |
             Spark SQL
                    |
          PostgreSQL Warehouse
                    |
             Apache Superset
                    |
          Interactive Dashboard (OpenSearch)
```

### Components

| Component | Role | Port |
|-----------|------|------|
| **Apache Airflow** | Schedules and orchestrates ingestion DAGs | 8081 |
| **Apache Kafka** | Streams raw market events between ingestion and Spark | 9092 (internal) / 9093 (host) |
| **Kafka UI** | Browse topics and messages | 8080 |
| **MinIO** | S3-compatible object store for raw JSON and Delta Lake | 9000 (API) / 9001 (console) |
| **Apache Spark** | Streaming jobs (Kafka → Delta Lake) + batch feature engineering | 7077 (master) / 8082 (UI) |
| **PostgreSQL** | Analytical warehouse — fact and dimension tables | 5430 (host) |
| **Apache Superset** | Interactive BI dashboard backed by PostgreSQL | 8088 |
| **OpenSearch** | Full-text search and real-time analytics index | 9200 |
| **OpenSearch Dashboards** | Real-time dashboard backed by OpenSearch | 5601 |
| **Prometheus** | Metrics scraping | 9090 |
| **Grafana** | Infrastructure monitoring | 3000 |

---

## Data Sources

| Asset class | Provider | Frequency |
|-------------|----------|-----------|
| Stock (AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA) | [Twelve Data](https://twelvedata.com) | every 15 min |
| Crypto (BTC, ETH, BNB, SOL, XRP) | [Binance](https://binance.com) | every 5 min |
| Forex (USD, EUR, GBP, JPY base) | [ExchangeRate-API](https://exchangerate-api.com) | every hour |

---

## Prerequisites

- Docker ≥ 24 and Docker Compose ≥ 2.20
- At least **8 GB RAM** allocated to Docker
- API keys in `.env` (see below)

---

## Quick Start

### 1. Configure environment

Copy and fill in your API keys:

```bash
cp .env .env.local   # optional — .env already has defaults for local dev
```

Key variables in `.env`:

```env
twelve_data_api_key=<your key>        # https://twelvedata.com  (800 req/day free)
exchange_rate_api_key=<your key>      # https://exchangerate-api.com (1500 req/month free)
binance_api_key=<your key>            # https://binance.com (public endpoints need no key)
```

### 2. Create the Docker network and start all services

```bash
docker compose up -d
```

Docker Compose creates the `financial_platform_network` bridge network automatically.

First boot takes a few minutes — Spark and OpenSearch download large images; the Airflow init container runs DB migrations before the scheduler starts.

### 3. Wait for services to be healthy

```bash
docker compose ps          # all services should show "running" or "healthy"
docker compose logs -f airflow-scheduler   # watch the scheduler come up
```

### 4. Trigger ingestion manually (optional)

Airflow DAGs run on schedule automatically. To run immediately:

```bash
# Via Airflow UI → http://localhost:8081  (admin / admin)
# Or via CLI:
docker exec financial-platform-airflow-scheduler \
    airflow dags trigger stock_ingestion
docker exec financial-platform-airflow-scheduler \
    airflow dags trigger crypto_ingestion
docker exec financial-platform-airflow-scheduler \
    airflow dags trigger forex_ingestion
```

### 5. Run the Spark feature-engineering batch job

After enough data has been ingested into Delta Lake:

```bash
docker exec financial-platform-spark \
    spark-submit /opt/bitnami/spark/jobs/delta_to_postgres.py
```

This computes MA5 / MA20 / MA50 and RSI-14 per symbol and loads the results into PostgreSQL.

---

## Viewing Dashboards

### Apache Superset — BI Dashboard (PostgreSQL)

1. Open **http://localhost:8088**
2. Log in with `admin` / `admin`
3. Navigate to **Dashboards → Financial Market Dashboard**

The bootstrap script creates all charts and the dashboard automatically on first start. If it has not run yet:

```bash
docker exec financial-platform-superset \
    python /app/pythonpath/bootstrap.py
```

Available charts:

| Page | Charts |
|------|--------|
| **Live Prices** | Current, High, Low, Open, Close, Volume (stocks + crypto) |
| **Candlestick** | OHLC 5-minute candles (stocks + crypto) |
| **Rankings** | Top 10 Gainers / Top 10 Losers (24 h % change) |
| **Volume** | Hourly and daily volume bars per symbol |
| **Technical Indicators** | MA5 / MA20 / MA50 overlay + RSI-14 with overbought/oversold bands |
| **Correlation** | BTC vs ETH scatter · NASDAQ (MSFT proxy) vs BTC line |

### OpenSearch Dashboards — Real-time Dashboard

1. Open **http://localhost:5601**
2. No login required (security disabled in dev mode)
3. Navigate to **Dashboards → Financial Market Dashboard**

The `opensearch-setup` container runs once on startup and creates index templates, index patterns, visualisations, and the dashboard automatically.

To re-run setup manually:

```bash
docker exec financial-platform-opensearch-setup \
    python setup_dashboards.py
```

Available visualisations mirror the Superset charts but are powered by the live OpenSearch indices (`financial-stocks-*`, `financial-crypto-*`, `financial-forex-*`) fed in real time by the Kafka→OpenSearch bridge.

---

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|---------|
| `stock_raw` | Airflow stock DAG | Spark Streaming, OpenSearch Bridge |
| `crypto_raw` | Airflow crypto DAG | Spark Streaming, OpenSearch Bridge |
| `forex_raw` | Airflow forex DAG | Spark Streaming, OpenSearch Bridge |

Browse messages at **http://localhost:8080** (Kafka UI).

---

## Data Warehouse Schema

```
fact_stock_price   — OHLCV + MA5/MA20/MA50 + RSI per stock tick
fact_crypto        — price + 24h high/low/volume + MA/RSI per crypto tick
fact_forex         — exchange rate per currency pair
dim_company        — symbol metadata
dim_time           — date/time decomposition
dim_market         — market / asset class lookup
```

Views pre-computed for dashboards: `vw_stock_live`, `vw_crypto_live`, `vw_forex_live`, `vw_stock_performance` (gainers/losers), `vw_stock_volume_hourly`, `vw_stock_volume_daily`, `vw_moving_averages`, `vw_ohlc_stock`, `vw_ohlc_crypto`, `vw_correlation_btc_eth`, `vw_correlation_nasdaq_btc`.

---

---

## Stopping the Platform

```bash
docker compose down            # stop containers, keep volumes
docker compose down -v         # stop containers AND delete all data
```

---

## Convention

### Kafka Topics
```
stock_raw   crypto_raw   forex_raw
```

### Warehouse Schema
```
fact_stock_price   fact_crypto   fact_forex
dim_company        dim_time      dim_market
```

### Dashboard Metrics
- **Live Prices**: Current · High · Low · Open · Close · Volume
- **Candlestick**: OHLC
- **Top Gainers / Losers**: Ranking
- **Volume**: Hourly · Daily
- **Moving Average**: MA5 · MA20 · MA50
- **RSI**: Technical indicator
- **Correlation**: BTC vs ETH · Gold vs Bitcoin · NASDAQ vs Bitcoin
