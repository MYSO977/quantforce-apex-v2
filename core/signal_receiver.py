#!/usr/bin/env python3
"""
QuantForce Apex v2 — signal_receiver.py
节点: .18
功能: 接收 tech_scanner 的 HTTP POST 信号，写入 signals_raw
端口: 5800
"""

import json
import uuid
import logging
import psycopg2
import psycopg2.extras
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RECEIVER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def write_signal(data: dict):
    ticker  = data.get("ticker") or data.get("symbol", "UNKNOWN")
    price   = float(data.get("price", 0))
    rvol    = float(data.get("rvol", 0))
    vwap    = float(data.get("vwap", 0))
    macd    = float(data.get("macd", 0))
    score   = float(data.get("score", 7.5))
    source  = data.get("source", "tech_scanner")

    # 基본질量过滤
    if price <= 0 or rvol < 1.5:
        log.warning(f"信号质量不足，丢弃: {ticker} price={price} rvol={rvol}")
        return False

    # 账户和仓位
    account  = data.get("account", "ib_cash")
    currency = "CAD" if account == "bmo_resp" else "USD"
    position = 3000.0 if account == "bmo_resp" else 400.0
    qty      = max(1, int(position / price)) if price > 0 else 1
    cost     = round(qty * price, 2)

    features = {
        "price":    price,
        "rvol":     rvol,
        "vwap":     vwap,
        "macd":     macd,
        "open":     float(data.get("open", 0)),
        "score":    score,
        "ticker":   ticker,
        "source":   source,
        "currency": currency,
        "account":  account,
        "qty":      qty,
        "position": position,
        "cost":     cost,
    }
    if data.get("ema9"):
        features["ema9"] = float(data["ema9"])


