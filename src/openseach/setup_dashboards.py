import json
import logging
import os
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SetupDashboards")

OS_BASE = f"http://{os.getenv('OPENSEARCH_HOST')}:{os.getenv('OPENSEARCH_PORT')}"
OSD_BASE = f"http://{os.getenv('OPENSEARCH_DASHBOARDS_HOST')}:5602"

TEMPLATES_DIR = Path(__file__).parent / "index_templates"
HEADERS_OS = {"Content-Type": "application/json"}
HEADERS_OSD = {"Content-Type": "application/json", "osd-xsrf": "true"}


# ── wait helpers ──────────────────────────────────────────────────────────────

def _wait(url: str, name: str, retries: int = 60):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                log.info("%s is ready", name)
                return
        except requests.ConnectionError:
            pass
        log.info("Waiting for %s (%d/%d) …", name, i + 1, retries)
        time.sleep(5)
    raise RuntimeError(f"{name} is not ready")


# ── OpenSearch index templates ────────────────────────────────────────────────

def apply_templates():
    for tpl_file in TEMPLATES_DIR.glob("*.json"):
        body = json.loads(tpl_file.read_text())
        name = tpl_file.stem
        r = requests.put(
            f"{OS_BASE}/_index_template/{name}",
            headers=HEADERS_OS,
            json=body,
        )
        if r.ok:
            log.info("Template '%s' applied", name)
        else:
            log.warning("Template '%s' failed: %s", name, r.text[:120])


# ── OpenSearch Dashboards saved objects ───────────────────────────────────────

def _osd_post(path: str, body: dict) -> dict:
    r = requests.post(f"{OSD_BASE}{path}", headers=HEADERS_OSD, json=body)
    r.raise_for_status()
    return r.json()


def _osd_get(path: str) -> dict:
    r = requests.get(f"{OSD_BASE}{path}", headers=HEADERS_OSD)
    r.raise_for_status()
    return r.json()


def create_index_patterns():
    patterns = [
        {"id": "financial-stocks-*",         "title": "financial-stocks-*",         "timeField": "@timestamp"},
        {"id": "financial-crypto-*",          "title": "financial-crypto-*",          "timeField": "@timestamp"},
        {"id": "financial-forex-*",           "title": "financial-forex-*",           "timeField": "@timestamp"},
        {"id": "financial-ml-predictions-*",  "title": "financial-ml-predictions-*",  "timeField": "@timestamp"},
    ]
    for p in patterns:
        try:
            _osd_post(f"/api/saved_objects/index-pattern/{p['id']}", {
                "attributes": {"title": p["title"], "timeFieldName": p["timeField"]},
            })
            log.info("Index pattern '%s' created", p["title"])
        except requests.HTTPError as e:
            log.warning("Index pattern '%s': %s", p["title"], e.response.text[:120])


def _vis(title: str, vis_type: str, index_id: str, aggs: list, params: dict) -> dict:
    """Build a minimal vis_state dict for a saved visualization."""
    return {
        "title": title,
        "type": vis_type,
        "params": params,
        "aggs": aggs,
    }


def create_visualizations() -> list[dict]:
    """Return list of (id, title) for created visualizations."""
    created = []

    def save_vis(vis_id: str, title: str, vis_state: dict, index_id: str) -> bool:
        body = {
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "",
                "savedSearchRefName": None,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": index_id,
                        "query": {"query": "", "language": "kuery"},
                        "filter": [],
                    }),
                },
            },
        }
        try:
            _osd_post(f"/api/saved_objects/visualization/{vis_id}", body)
            log.info("Visualization '%s' created", title)
            created.append({"id": vis_id, "title": title})
            return True
        except requests.HTTPError as e:
            log.warning("Visualization '%s': %s", title, e.response.text[:120])
            return False

    # Live Prices — data table
    save_vis("vis-stock-live-prices", "Live Stock Prices", {
        "title": "Live Stock Prices",
        "type": "table",
        "params": {
            "perPage": 20, "showPartialRows": False,
            "showMeticsAtAllLevels": False, "sort": {"columnIndex": None, "direction": None},
            "showTotal": False, "totalFunc": "sum",
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "price", "customLabel": "Price"}},
            {"id": "2", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "high_price", "customLabel": "High"}},
            {"id": "3", "enabled": True, "type": "min", "schema": "metric", "params": {"field": "low_price",  "customLabel": "Low"}},
            {"id": "4", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "open_price", "customLabel": "Open"}},
            {"id": "5", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "close_price","customLabel": "Close"}},
            {"id": "6", "enabled": True, "type": "sum", "schema": "metric", "params": {"field": "volume",     "customLabel": "Volume"}},
            {"id": "7", "enabled": True, "type": "terms","schema": "bucket", "params": {"field": "symbol",    "customLabel": "Symbol", "size": 50}},
        ],
    }, "financial-stocks-*")

    # Live Crypto Prices
    save_vis("vis-crypto-live-prices", "Live Crypto Prices", {
        "title": "Live Crypto Prices",
        "type": "table",
        "params": {"perPage": 10},
        "aggs": [
            {"id": "1", "enabled": True, "type": "max",   "schema": "metric", "params": {"field": "price",      "customLabel": "Price"}},
            {"id": "2", "enabled": True, "type": "max",   "schema": "metric", "params": {"field": "high_24h",   "customLabel": "24h High"}},
            {"id": "3", "enabled": True, "type": "min",   "schema": "metric", "params": {"field": "low_24h",    "customLabel": "24h Low"}},
            {"id": "4", "enabled": True, "type": "sum",   "schema": "metric", "params": {"field": "volume_24h", "customLabel": "24h Volume"}},
            {"id": "5", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "symbol",     "customLabel": "Symbol", "size": 20}},
        ],
    }, "financial-crypto-*")

    # Stock price line
    save_vis("vis-stock-price-trend", "Stock Price Trend", {
        "title": "Stock Price Trend",
        "type": "line",
        "params": {
            "type": "line", "grid": {"categoryLines": False},
            "categoryAxes": [{"scale": {"type": "linear"}, "title": {}}],
            "valueAxes": [{"title": {"text": "Price (USD)"}, "scale": {"mode": "normal"}}],
            "seriesParams": [{"type": "line", "mode": "normal", "interpolate": "linear"}],
            "addTooltip": True, "addLegend": True, "legendPosition": "right",
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg",   "schema": "metric", "params": {"field": "price", "customLabel": "Price"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "customLabel": "Time"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "symbol", "size": 10, "customLabel": "Symbol"}},
        ],
    }, "financial-stocks-*")

    # Crypto price line
    save_vis("vis-crypto-price-trend", "Crypto Price Trend", {
        "title": "Crypto Price Trend",
        "type": "line",
        "params": {"addTooltip": True, "addLegend": True, "legendPosition": "right"},
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "price", "customLabel": "Price"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "symbol", "size": 5}},
        ],
    }, "financial-crypto-*")

    # Moving Averages
    save_vis("vis-moving-averages", "Moving Averages (MA5 / MA20 / MA50)", {
        "title": "Moving Averages",
        "type": "line",
        "params": {"addTooltip": True, "addLegend": True},
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "ma5",  "customLabel": "MA5"}},
            {"id": "2", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "ma20", "customLabel": "MA20"}},
            {"id": "3", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "ma50", "customLabel": "MA50"}},
            {"id": "4", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "price","customLabel": "Price"}},
            {"id": "5", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "6", "enabled": True, "type": "terms", "schema": "group", "params": {"field": "symbol", "size": 5}},
        ],
    }, "financial-stocks-*")

    # RSI
    save_vis("vis-rsi", "RSI-14 Technical Indicator", {
        "title": "RSI-14",
        "type": "line",
        "params": {
            "addTooltip": True, "addLegend": True,
            "valueAxes": [{"title": {"text": "RSI"}, "scale": {"min": 0, "max": 100}}],
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "rsi", "customLabel": "RSI"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group", "params": {"field": "symbol", "size": 5}},
        ],
    }, "financial-stocks-*")

    # Hourly Volume
    save_vis("vis-hourly-volume", "Hourly Volume", {
        "title": "Hourly Volume",
        "type": "histogram",
        "params": {"addTooltip": True, "addLegend": True},
        "aggs": [
            {"id": "1", "enabled": True, "type": "sum", "schema": "metric", "params": {"field": "volume", "customLabel": "Volume"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "h"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group", "params": {"field": "symbol", "size": 5}},
        ],
    }, "financial-stocks-*")

    # Daily Volume
    save_vis("vis-daily-volume", "Daily Volume", {
        "title": "Daily Volume",
        "type": "histogram",
        "params": {"addTooltip": True, "addLegend": True},
        "aggs": [
            {"id": "1", "enabled": True, "type": "sum", "schema": "metric", "params": {"field": "volume", "customLabel": "Volume"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "d"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group", "params": {"field": "symbol", "size": 5}},
        ],
    }, "financial-stocks-*")

    # BTC vs ETH scatter
    save_vis("vis-btc-eth-correlation", "BTC vs ETH Correlation", {
        "title": "BTC vs ETH Correlation",
        "type": "line",
        "params": {"addTooltip": True, "addLegend": True},
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "price", "customLabel": "Price"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "5m"}},
            {"id": "3", "enabled": True, "type": "filters", "schema": "group",
             "params": {"filters": [
                 {"input": {"query": "symbol:BTCUSDT"}, "label": "BTC"},
                 {"input": {"query": "symbol:ETHUSDT"}, "label": "ETH"},
             ]}},
        ],
    }, "financial-crypto-*")

    # Forex live rates
    save_vis("vis-forex-rates", "Forex Live Rates", {
        "title": "Forex Live Rates",
        "type": "table",
        "params": {"perPage": 20},
        "aggs": [
            {"id": "1", "enabled": True, "type": "max",   "schema": "metric", "params": {"field": "rate",           "customLabel": "Rate"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "base_currency",  "customLabel": "Base",   "size": 10}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "target_currency","customLabel": "Target", "size": 30}},
        ],
    }, "financial-forex-*")

    # ── ML Prediction visualizations ─────────────────────────────────────────

    # Top 5 predicted risers — horizontal bar sorted by confidence DESC
    save_vis("vis-ml-top-rise", "ML Top 5 Predicted Risers", {
        "title": "ML Top 5 Predicted Risers",
        "type": "horizontal_bar",
        "params": {
            "addTooltip": True, "addLegend": False,
            "categoryAxes": [{"title": {"text": "Symbol"}}],
            "valueAxes": [{"title": {"text": "P(RISE)"}, "scale": {"min": 0, "max": 1}}],
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "confidence", "customLabel": "P(RISE)"}},
            {"id": "2", "enabled": True, "type": "filters", "schema": "segment",
             "params": {"filters": [{"input": {"query": "category:TOP_RISE"}, "label": "TOP_RISE"}]}},
            {"id": "3", "enabled": True, "type": "terms",  "schema": "group",
             "params": {"field": "symbol", "size": 5, "order": "desc", "orderBy": "1", "customLabel": "Symbol"}},
        ],
    }, "financial-ml-predictions-*")

    # Top 5 predicted droppers — horizontal bar sorted by confidence ASC
    save_vis("vis-ml-top-drop", "ML Top 5 Predicted Droppers", {
        "title": "ML Top 5 Predicted Droppers",
        "type": "horizontal_bar",
        "params": {
            "addTooltip": True, "addLegend": False,
            "categoryAxes": [{"title": {"text": "Symbol"}}],
            "valueAxes": [{"title": {"text": "P(RISE) ← low = likely drop"}, "scale": {"min": 0, "max": 1}}],
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "min",   "schema": "metric",
             "params": {"field": "confidence", "customLabel": "P(RISE)"}},
            {"id": "2", "enabled": True, "type": "filters", "schema": "segment",
             "params": {"filters": [{"input": {"query": "category:TOP_DROP"}, "label": "TOP_DROP"}]}},
            {"id": "3", "enabled": True, "type": "terms",  "schema": "group",
             "params": {"field": "symbol", "size": 5, "order": "asc", "orderBy": "1", "customLabel": "Symbol"}},
        ],
    }, "financial-ml-predictions-*")

    # Model agreement — data table showing all latest predictions
    save_vis("vis-ml-predictions-table", "ML Predictions — All Symbols", {
        "title": "ML Predictions — All Symbols",
        "type": "table",
        "params": {"perPage": 20, "showPartialRows": False, "showTotal": False},
        "aggs": [
            {"id": "1", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "confidence",      "customLabel": "Confidence"}},
            {"id": "2", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "rf_probability",  "customLabel": "RF P(RISE)"}},
            {"id": "3", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "gbt_probability", "customLabel": "GBT P(RISE)"}},
            {"id": "4", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "lr_probability",  "customLabel": "LR P(RISE)"}},
            {"id": "5", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "model_votes",     "customLabel": "Votes"}},
            {"id": "6", "enabled": True, "type": "max",   "schema": "metric",
             "params": {"field": "current_price",   "customLabel": "Price"}},
            {"id": "7", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "symbol", "size": 20, "customLabel": "Symbol"}},
            {"id": "8", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "prediction", "size": 2, "customLabel": "Signal"}},
        ],
    }, "financial-ml-predictions-*")

    # Confidence over time (trend of ensemble certainty)
    save_vis("vis-ml-confidence-trend", "ML Confidence Trend", {
        "title": "ML Confidence Trend",
        "type": "line",
        "params": {"addTooltip": True, "addLegend": True,
                   "valueAxes": [{"title": {"text": "P(RISE)"}, "scale": {"min": 0, "max": 1}}]},
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
             "params": {"field": "confidence", "customLabel": "Avg P(RISE)"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "h"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "symbol", "size": 10}},
        ],
    }, "financial-ml-predictions-*")

    return created


def create_dashboard(vis_list: list[dict]):
    panels = []
    cols, col_width, row_height = 2, 24, 15
    for i, v in enumerate(vis_list):
        col = i % cols
        row = i // cols
        panels.append({
            "panelIndex": str(i + 1),
            "gridData": {"x": col * col_width, "y": row * row_height, "w": col_width, "h": row_height, "i": str(i + 1)},
            "type": "visualization",
            "version": "2.17.0",
            "panelRefName": f"panel_{i}",
        })

    refs = [
        {"name": f"panel_{i}", "type": "visualization", "id": v["id"]}
        for i, v in enumerate(vis_list)
    ]

    body = {
        "attributes": {
            "title": "Financial Market Dashboard",
            "hits": 0,
            "description": "Live prices, OHLC, volume, MA, RSI, correlations",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True}),
            "version": 1,
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
        },
        "references": refs,
    }
    try:
        _osd_post("/api/saved_objects/dashboard/financial-market-dashboard", body)
        log.info("Dashboard 'Financial Market Dashboard' created")
    except requests.HTTPError as e:
        log.warning("Dashboard creation: %s", e.response.text[:200])


def run():
    _wait(f"{OS_BASE}/_cluster/health",  "OpenSearch")
    _wait(f"{OSD_BASE}/api/status",      "OpenSearch Dashboards")
    time.sleep(5)

    apply_templates()
    create_index_patterns()
    vis_list = create_visualizations()
    create_dashboard(vis_list)
    log.info("Setup complete.")


if __name__ == "__main__":
    run()
