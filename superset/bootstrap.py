import json
import os
import time

import requests

SUPERSET_BASE = "http://localhost:8086"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"
PG_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://finance:finance@postgres:5432/finance",
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _wait_for_superset(max_wait: int = 120):
    for _ in range(max_wait):
        try:
            r = requests.get(f"{SUPERSET_BASE}/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    raise RuntimeError("Superset did not become healthy in time")


def _login() -> dict:
    r = requests.post(
        f"{SUPERSET_BASE}/api/v1/security/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS, "provider": "db", "refresh": True},
    )
    r.raise_for_status()
    token = r.json()["access_token"]

    csrf_r = requests.get(
        f"{SUPERSET_BASE}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
    )
    csrf_r.raise_for_status()
    csrf = csrf_r.json()["result"]

    return {
        "Authorization": f"Bearer {token}",
        "X-CSRFToken": csrf,
        "Content-Type": "application/json",
        "Referer": SUPERSET_BASE,
    }


def _post(path: str, headers: dict, body: dict) -> dict:
    r = requests.post(f"{SUPERSET_BASE}{path}", headers=headers, json=body)
    r.raise_for_status()
    return r.json()


def _metric(col: str, agg: str = "AVG", label: str | None = None) -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
        "hasCustomLabel": label is not None,
    }


def _layout(chart_ids: list[int]) -> str:
    """Build a two-column responsive dashboard layout."""
    rows = []
    children_of_grid = []
    for i, cid in enumerate(chart_ids):
        row_id = f"ROW-{i}"
        chart_key = f"CHART-{i}"
        rows.append({
            row_id: {
                "type": "ROW",
                "id": row_id,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "children": [chart_key],
                "parents": ["GRID_ID"],
            },
            chart_key: {
                "type": "CHART",
                "id": chart_key,
                "meta": {"chartId": cid, "width": 12, "height": 50},
                "parents": ["GRID_ID", row_id],
            },
        })
        children_of_grid.append(row_id)

    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": children_of_grid,
            "parents": ["ROOT_ID"],
        },
    }
    for row in rows:
        position.update(row)
    return json.dumps(position)


# ── main bootstrap ────────────────────────────────────────────────────────────

def bootstrap():
    print("Waiting for Superset …")
    _wait_for_superset()
    time.sleep(5)  # give gunicorn workers time to settle

    h = _login()

    # 1. Database connection
    db_resp = _post("/api/v1/database/", h, {
        "database_name": "Finance Warehouse",
        "sqlalchemy_uri": PG_URI,
        "expose_in_sqllab": True,
        "allow_run_async": True,
    })
    db_id = db_resp["id"]
    print(f"Created database id={db_id}")

    # 2. Datasets
    tables = [
        "fact_stock_price", "fact_crypto", "fact_forex",
        "vw_stock_live", "vw_crypto_live", "vw_forex_live",
        "vw_stock_performance", "vw_stock_volume_hourly", "vw_stock_volume_daily",
        "vw_moving_averages", "vw_ohlc_stock", "vw_ohlc_crypto",
        "vw_correlation_btc_eth", "vw_correlation_nasdaq_btc",
        # ML prediction views
        "ml_stock_predictions", "vw_ml_top_movers", "vw_ml_model_agreement",
    ]
    ds_ids: dict[str, int] = {}
    for tbl in tables:
        try:
            resp = _post("/api/v1/dataset/", h, {
                "database": db_id,
                "table_name": tbl,
                "schema": "public",
            })
            ds_ids[tbl] = resp["id"]
            print(f"  Dataset {tbl} id={resp['id']}")
        except requests.HTTPError as e:
            print(f"  Dataset {tbl} skipped: {e.response.text[:120]}")

    # 3. Charts
    def make_chart(name: str, viz: str, ds: str, params: dict) -> int | None:
        if ds not in ds_ids:
            print(f"  Skipped chart '{name}' (dataset '{ds}' not created)")
            return None
        try:
            resp = _post("/api/v1/chart/", h, {
                "slice_name": name,
                "viz_type": viz,
                "datasource_id": ds_ids[ds],
                "datasource_type": "table",
                "params": json.dumps(params),
                "owners": [1],
            })
            cid = resp["id"]
            print(f"  Chart '{name}' id={cid}")
            return cid
        except requests.HTTPError as e:
            print(f"  Chart '{name}' failed: {e.response.text[:120]}")
            return None

    chart_ids: list[int] = []

    # — Live Prices table (stock) ————————————————————————————————
    cid = make_chart("Live Stock Prices", "table", "vw_stock_live", {
        "groupby": ["symbol", "event_time"],
        "metrics": [
            _metric("price", "MAX", "Price"),
            _metric("open_price", "MAX", "Open"),
            _metric("high_price", "MAX", "High"),
            _metric("low_price", "MIN", "Low"),
            _metric("close_price", "MAX", "Close"),
            _metric("volume", "SUM", "Volume"),
        ],
        "order_desc": True,
        "time_range": "No filter",
    })
    if cid: chart_ids.append(cid)

    # — Live Prices table (crypto) ——————————————————————————————
    cid = make_chart("Live Crypto Prices", "table", "vw_crypto_live", {
        "groupby": ["symbol", "event_time"],
        "metrics": [
            _metric("price", "MAX", "Price"),
            _metric("high_24h", "MAX", "24h High"),
            _metric("low_24h", "MIN", "24h Low"),
            _metric("volume_24h", "SUM", "24h Volume"),
        ],
        "time_range": "No filter",
    })
    if cid: chart_ids.append(cid)

    # — Price trend line (stocks) ————————————————————————————————
    cid = make_chart("Stock Price Trend", "echarts_timeseries_line", "fact_stock_price", {
        "metrics": [_metric("price", "AVG", "Price")],
        "groupby": ["symbol"],
        "granularity_sqla": "event_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "show_legend": True,
        "rich_tooltip": True,
        "y_axis_title": "Price (USD)",
    })
    if cid: chart_ids.append(cid)

    # — Crypto price trend ——————————————————————————————————————
    cid = make_chart("Crypto Price Trend", "echarts_timeseries_line", "fact_crypto", {
        "metrics": [_metric("price", "AVG", "Price")],
        "groupby": ["symbol"],
        "granularity_sqla": "event_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "show_legend": True,
        "rich_tooltip": True,
    })
    if cid: chart_ids.append(cid)

    # — Candlestick (stocks) ————————————————————————————————————
    cid = make_chart("Stock OHLC Candlestick", "echarts_timeseries_candlestick", "vw_ohlc_stock", {
        "granularity_sqla": "candle_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "metrics": [_metric("close", "MAX", "Close")],
        "groupby": ["symbol"],
        "open": _metric("open", "MAX", "Open"),
        "close": _metric("close", "MAX", "Close"),
        "high": _metric("high", "MAX", "High"),
        "low": _metric("low", "MIN", "Low"),
    })
    if cid: chart_ids.append(cid)

    # — Candlestick (crypto) ————————————————————————————————————
    cid = make_chart("Crypto OHLC Candlestick", "echarts_timeseries_candlestick", "vw_ohlc_crypto", {
        "granularity_sqla": "candle_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "metrics": [_metric("close", "MAX", "Close")],
        "groupby": ["symbol"],
        "open": _metric("open", "MAX", "Open"),
        "close": _metric("close", "MAX", "Close"),
        "high": _metric("high", "MAX", "High"),
        "low": _metric("low", "MIN", "Low"),
    })
    if cid: chart_ids.append(cid)

    # — Top Gainers ————————————————————————————————————————————
    cid = make_chart("Top Gainers (24h %)", "echarts_bar", "vw_stock_performance", {
        "metrics": [_metric("pct_change", "MAX", "% Change")],
        "groupby": ["symbol"],
        "adhoc_filters": [{
            "expressionType": "SIMPLE",
            "subject": "pct_change",
            "operator": ">",
            "comparator": "0",
        }],
        "order_desc": True,
        "row_limit": 10,
        "time_range": "No filter",
        "y_axis_title": "% Change",
    })
    if cid: chart_ids.append(cid)

    # — Top Losers ——————————————————————————————————————————————
    cid = make_chart("Top Losers (24h %)", "echarts_bar", "vw_stock_performance", {
        "metrics": [_metric("pct_change", "MAX", "% Change")],
        "groupby": ["symbol"],
        "adhoc_filters": [{
            "expressionType": "SIMPLE",
            "subject": "pct_change",
            "operator": "<",
            "comparator": "0",
        }],
        "order_desc": False,
        "row_limit": 10,
        "time_range": "No filter",
        "y_axis_title": "% Change",
    })
    if cid: chart_ids.append(cid)

    # — Hourly Volume ————————————————————————————————————————————
    cid = make_chart("Hourly Volume (Stock)", "echarts_timeseries_bar", "vw_stock_volume_hourly", {
        "metrics": [_metric("total_volume", "SUM", "Volume")],
        "groupby": ["symbol"],
        "granularity_sqla": "hour_bucket",
        "time_grain_sqla": "PT1H",
        "time_range": "Last week",
    })
    if cid: chart_ids.append(cid)

    # — Daily Volume ————————————————————————————————————————————
    cid = make_chart("Daily Volume (Stock)", "echarts_timeseries_bar", "vw_stock_volume_daily", {
        "metrics": [_metric("total_volume", "SUM", "Volume")],
        "groupby": ["symbol"],
        "granularity_sqla": "day_bucket",
        "time_grain_sqla": "P1D",
        "time_range": "Last month",
    })
    if cid: chart_ids.append(cid)

    # — Moving Averages ————————————————————————————————————————
    cid = make_chart("Moving Averages (MA5 / MA20 / MA50)", "echarts_timeseries_line", "vw_moving_averages", {
        "metrics": [
            _metric("ma5", "AVG", "MA5"),
            _metric("ma20", "AVG", "MA20"),
            _metric("ma50", "AVG", "MA50"),
            _metric("price", "AVG", "Price"),
        ],
        "groupby": ["symbol"],
        "granularity_sqla": "event_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "show_legend": True,
    })
    if cid: chart_ids.append(cid)

    # — RSI ——————————————————————————————————————————————————————
    cid = make_chart("RSI-14 Technical Indicator", "echarts_timeseries_line", "vw_moving_averages", {
        "metrics": [_metric("rsi", "AVG", "RSI")],
        "groupby": ["symbol"],
        "granularity_sqla": "event_time",
        "time_grain_sqla": "PT5M",
        "time_range": "Last day",
        "show_legend": True,
        "y_axis_bounds": [0, 100],
        "y_axis_title": "RSI",
        "annotationLayers": [
            {"value": 70, "name": "Overbought (70)", "style": "dashed", "color": "red"},
            {"value": 30, "name": "Oversold (30)",   "style": "dashed", "color": "green"},
        ],
    })
    if cid: chart_ids.append(cid)

    # — BTC vs ETH Correlation ————————————————————————————————
    cid = make_chart("BTC vs ETH Correlation", "scatter", "vw_correlation_btc_eth", {
        "x": _metric("btc_price", "MAX", "BTC Price"),
        "y": _metric("eth_price", "MAX", "ETH Price"),
        "time_range": "Last week",
        "x_axis_label": "BTC Price (USDT)",
        "y_axis_label": "ETH Price (USDT)",
    })
    if cid: chart_ids.append(cid)

    # — NASDAQ vs BTC ——————————————————————————————————————————
    cid = make_chart("NASDAQ (MSFT) vs Bitcoin", "echarts_timeseries_line", "vw_correlation_nasdaq_btc", {
        "metrics": [
            _metric("nasdaq_price", "AVG", "MSFT (NASDAQ proxy)"),
            _metric("btc_price", "AVG", "BTC"),
        ],
        "granularity_sqla": "bucket",
        "time_grain_sqla": "PT1H",
        "time_range": "Last week",
        "show_legend": True,
    })
    if cid: chart_ids.append(cid)

    # ── ML Prediction charts ─────────────────────────────────────────────────

    # Top 5 predicted risers
    cid = make_chart("ML Top 5 Predicted Risers", "echarts_bar", "vw_ml_top_movers", {
        "metrics": [_metric("confidence", "MAX", "P(RISE)")],
        "groupby": ["symbol"],
        "adhoc_filters": [{
            "expressionType": "SIMPLE",
            "subject": "category",
            "operator": "==",
            "comparator": "TOP_RISE",
        }],
        "order_desc": True,
        "row_limit": 5,
        "time_range": "No filter",
        "y_axis_title": "Confidence (P RISE)",
        "y_axis_bounds": [0, 1],
    })
    if cid: chart_ids.append(cid)

    # Top 5 predicted droppers
    cid = make_chart("ML Top 5 Predicted Droppers", "echarts_bar", "vw_ml_top_movers", {
        "metrics": [_metric("confidence", "MIN", "P(RISE)")],
        "groupby": ["symbol"],
        "adhoc_filters": [{
            "expressionType": "SIMPLE",
            "subject": "category",
            "operator": "==",
            "comparator": "TOP_DROP",
        }],
        "order_desc": False,
        "row_limit": 5,
        "time_range": "No filter",
        "y_axis_title": "Confidence (P RISE) ← low = likely drop",
        "y_axis_bounds": [0, 1],
    })
    if cid: chart_ids.append(cid)

    # Full predictions table (latest per symbol)
    cid = make_chart("ML Predictions — All Symbols", "table", "vw_ml_model_agreement", {
        "groupby": ["symbol", "prediction", "agreement_level"],
        "metrics": [
            _metric("confidence", "MAX", "Confidence"),
            _metric("model_votes", "MAX", "Votes (RISE)"),
            _metric("current_price", "MAX", "Current Price"),
        ],
        "order_desc": True,
        "time_range": "No filter",
    })
    if cid: chart_ids.append(cid)

    # Per-model probability comparison
    cid = make_chart("ML Model Probability Comparison", "table", "vw_ml_top_movers", {
        "groupby": ["symbol", "category", "prediction"],
        "metrics": [
            _metric("rf_probability",  "MAX", "RF P(RISE)"),
            _metric("gbt_probability", "MAX", "GBT P(RISE)"),
            _metric("lr_probability",  "MAX", "LR P(RISE)"),
            _metric("confidence",      "MAX", "Ensemble"),
        ],
        "order_desc": True,
        "time_range": "No filter",
    })
    if cid: chart_ids.append(cid)

    # 4. Dashboard
    try:
        dash_resp = _post("/api/v1/dashboard/", h, {
            "dashboard_title": "Financial Market Dashboard",
            "owners": [1],
            "published": True,
            "position_json": _layout(chart_ids),
        })
        print(f"Dashboard created id={dash_resp['id']}")
    except requests.HTTPError as e:
        print(f"Dashboard creation failed: {e.response.text[:200]}")


if __name__ == "__main__":
    bootstrap()
