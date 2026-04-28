#!/usr/bin/env python3
"""
QuantForce Apex v2 — panel_api.py
节点: .18
端口: 5801
iPad 监控面板数据接口
"""

import json
import psycopg2
import psycopg2.extras
import subprocess
import time
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"

NODES = [
    {"id": ".11", "ip": "192.168.0.11", "name": "Dell OptiPlex", "role": "执行器+GPU"},
    {"id": ".18", "ip": "192.168.0.18", "name": "Acer XC-605",  "role": "中央大脑"},
    {"id": ".143","ip": "192.168.0.143","name": "Lenovo",        "role": "扫描器"},
    {"id": ".101","ip": "192.168.0.101","name": "Asus L406M",    "role": "哨兵"},
    {"id": ".102","ip": "192.168.0.102","name": "Asus L410M",    "role": "信使"},
]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [PANEL] %(message)s")
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=3
    )


def ping_node(ip: str) -> bool:
    try:
        r = subprocess.run(["ping", "-c", "1", "-W", "1", ip],
                           capture_output=True, timeout=2)
        return r.returncode == 0
    except:
        return False


def get_signal_stats() -> dict:
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 今日信号统计
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24h') as today_total,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24h' AND signal_type='tech') as today_tech,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24h' AND signal_type='news') as today_news,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24h' AND direction='buy' AND status='pending' AND notified=FALSE) as pending,
                    AVG(score) FILTER (WHERE created_at > NOW() - INTERVAL '24h') as avg_score
                FROM signals_raw
            """)
            stats = dict(cur.fetchone())

            # 最新5条信号
            cur.execute("""
                SELECT symbol, direction, score, llm_score, gpu_score,
                       signal_type, source, created_at
                FROM signals_raw
                WHERE direction = 'buy'
                ORDER BY created_at DESC
                LIMIT 5
            """)
            recent = []
            for r in cur.fetchall():
                d = dict(r)
                d["created_at"] = d["created_at"].isoformat()
                recent.append(d)

            # t1_positions 持仓
            cur.execute("""
                SELECT symbol, amount_usd, buy_date, settle_date, status
                FROM t1_positions
                WHERE status = 'open'
                ORDER BY buy_date DESC
            """)
            positions = []
            for r in cur.fetchall():
                d = dict(r)
                d["buy_date"]    = str(d["buy_date"])
                d["settle_date"] = str(d["settle_date"])
                positions.append(d)

        conn.close()
        return {
            "stats":     stats,
            "recent":    recent,
            "positions": positions,
        }
    except Exception as e:
        log.error(f"DB错误: {e}")
        return {"stats": {}, "recent": [], "positions": []}


def get_dashboard_data() -> dict:
    now = datetime.now(ET)

    # 节点状态
    nodes = []
    for n in NODES:
        alive = ping_node(n["ip"])
        nodes.append({**n, "alive": alive})

    # 信号数据
    signal_data = get_signal_stats()

    # 市场状态
    is_weekend = now.weekday() >= 5
    market_open  = now.replace(hour=9, minute=30, second=0)
    market_close = now.replace(hour=16, minute=0, second=0)
    market_status = "closed"
    if not is_weekend:
        if market_open <= now <= market_close:
            market_status = "open"
        elif now < market_open:
            market_status = "pre"
        else:
            market_status = "after"

    return {
        "ts":            now.isoformat(),
        "market_status": market_status,
        "nodes":         nodes,
        "signals":       signal_data,
        "accounts": {
            "ib_cash": {
                "balance": 1200,
                "positions": len(signal_data.get("positions", [])),
                "max_positions": 3,
            },
            "bmo_resp": {
                "balance": 18000,
                "currency": "CAD",
            }
        }
    }


class PanelHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/dashboard":
            data = get_dashboard_data()
            self._json(200, data)
        elif self.path == "/health":
            self._json(200, {"status": "ok"})
        elif self.path == "/" or self.path == "/dashboard":
            self._html()
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, code, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self):
        html = open("/home/heng/quantforce-apex-v2/dashboard/QuantForce_Apex_v2.html", "rb").read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5801), PanelHandler)
    log.info("panel_api 启动，监听 0.0.0.0:5801")
    server.serve_forever()
