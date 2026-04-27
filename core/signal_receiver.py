#!/usr/bin/env python3
"""
QuantForce Apex v2 — signal_receiver.py
节点: .18 | 端口: 5800
接收 tech_scanner HTTP POST，自动计算股数，写入 signals_raw
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
    ticker   = data.get("ticker") or data.get("symbol", "UNKNOWN")
    price    = float(data.get("price", 0))
    rvol     = float(data.get("rvol", 0))
    vwap     = float(data.get("vwap", 0))
    macd     = float(data.get("macd", 0))
    score    = float(data.get("score", 7.5))
    source   = data.get("source", "tech_scanner")
    account  = data.get("account", "ib_cash")
    currency = "CAD" if account == "bmo_resp" else "USD"

    # 基本质量过滤
    if price <= 0 or rvol < 1.5:
        log.warning(f"信号质量不足，丢弃: {ticker} price={price} rvol={rvol}")
        return False

    # 自动计算股数
    position = 3000.0 if account == "bmo_resp" else 400.0
    qty      = max(1, int(position / price))
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

    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO signals_raw
                  (signal_id, symbol, signal_type, direction,
                   confidence, score, source, pipeline, features)
                VALUES (%s, %s, 'tech', 'buy', %s, %s, %s, 'apex', %s)
                ON CONFLICT (signal_id) DO NOTHING
            """, (
                str(uuid.uuid4()),
                ticker,
                min(score, 10.0),
                score,
                source,
                psycopg2.extras.Json(features)
            ))
        conn.commit()
        conn.close()
        log.info(f"✅ 写入PG: {ticker} qty={qty} price={price} cost={cost} rvol={rvol}")
        return True
    except Exception as e:
        log.error(f"写入失败: {e}")
        return False


class SignalHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/signal":
            self.send_response(404)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            data   = json.loads(body)
            ok     = write_signal(data)
            self.send_response(200 if ok else 400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": ok}).encode())
        except Exception as e:
            log.error(f"请求处理失败: {e}")
            self.send_response(500)
            self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5800), SignalHandler)
    log.info("signal_receiver 启动，监听 0.0.0.0:5800")
    server.serve_forever()
