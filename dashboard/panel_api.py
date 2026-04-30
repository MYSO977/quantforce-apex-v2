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
import paramiko
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
                SELECT s.symbol, s.direction, s.score, s.llm_score, s.gpu_score,
                       s.signal_type, s.source, s.created_at, s.features, s.gpu_indicators,
                       w.dollar_volume_rank, w.sector as w_sector, w.active as in_whitelist
                FROM signals_raw s
                LEFT JOIN universe_whitelist w ON s.symbol = w.symbol
                WHERE s.direction = 'buy'
                  AND s.signal_type = 'tech'
                  AND s.features->>'price' IS NOT NULL
                  AND s.created_at > NOW() - INTERVAL '24h'
                ORDER BY (
                    COALESCE(score,0) + COALESCE(llm_score,0) + COALESCE(gpu_score,0)
                ) / NULLIF(
                    (CASE WHEN score>0 THEN 1 ELSE 0 END +
                     CASE WHEN llm_score>0 THEN 1 ELSE 0 END +
                     CASE WHEN gpu_score>0 THEN 1 ELSE 0 END), 0
                ) DESC NULLS LAST,
                created_at DESC
                LIMIT 50
            """)
            recent = []
            for r in cur.fetchall():
                d = dict(r)
                d["created_at"] = d["created_at"].isoformat()
                f = d.get("features") or {}
                d["rvol"]           = f.get("rvol")
                d["price"]          = f.get("price")
                d["macd"]           = f.get("macd") or f.get("macd_hist")
                d["vwap"]           = f.get("vwap")
                d["currency"]       = f.get("currency","USD")
                price = f.get("price")
                sl = f.get("stop_loss")
                tp = f.get("take_profit")
                if price and not sl:
                    sl = round(price * 0.97, 2)
                if price and not tp:
                    tp = round(price * 1.12, 2)
                sl_pct = f.get("stop_loss_pct") or (round((sl/price-1)*100,1) if price and sl else None)
                tp_pct = f.get("take_profit_pct") or (round((tp/price-1)*100,1) if price and tp else None)
                d["stop_loss"]      = sl
                d["take_profit"]    = tp
                d["stop_loss_pct"]  = sl_pct
                d["take_profit_pct"]= tp_pct
                currency = f.get("currency", "USD")
                if currency == "USD":
                    if price and price > 25:
                        recent = recent  # 后面用 continue 跳过
                        d["_skip"] = True
                    elif price and price > 0:
                        d["shares"] = int(400 / price)
                        d["position_value"] = round(d["shares"] * price, 2)
                    else:
                        d["shares"] = f.get("shares") or f.get("qty")
                elif currency == "CAD":
                    if price and price > 50:
                        d["_skip"] = True
                    elif price and price > 0:
                        d["shares"] = int(2500 / price)
                        d["position_value"] = round(d["shares"] * price, 2)
                    else:
                        d["shares"] = f.get("shares") or f.get("qty")
                else:
                    d["shares"] = f.get("shares") or f.get("qty")
                # 横盘突破检测
                bb_upper = d.get("bb_upper") or (d.get("gpu_indicators") or {}).get("bb_upper")
                bb_lower = d.get("bb_lower") or (d.get("gpu_indicators") or {}).get("bb_lower")
                bb_mid   = d.get("bb_mid")   or (d.get("gpu_indicators") or {}).get("bb_mid")
                ema9_slope = (d.get("gpu_indicators") or {}).get("ema9_slope")
                if bb_upper and bb_lower and bb_mid and price:
                    bb_width_pct = round((bb_upper - bb_lower) / bb_mid * 100, 2)
                    is_squeeze = bb_width_pct < 5.0
                    is_breakout = price >= bb_upper * 0.995
                    slope_flat = ema9_slope is not None and abs(ema9_slope) < 0.3
                    d["bb_width_pct"] = bb_width_pct
                    d["consolidation"] = is_squeeze and is_breakout
                    d["consolidation_detail"] = {
                        "bb_width_pct": bb_width_pct,
                        "is_squeeze": is_squeeze,
                        "is_breakout": is_breakout,
                        "slope_flat": slope_flat
                    }
                else:
                    d["bb_width_pct"] = None
                    d["consolidation"] = False
                    d["consolidation_detail"] = None
                d["dollar_volume_rank"] = d.get("dollar_volume_rank")
                d["in_whitelist"]   = d.get("in_whitelist") or False
                d["position_cad"]   = f.get("position_cad") or f.get("cost")
                d["sector"]         = f.get("sector","")
                g = d.get("gpu_indicators") or {}
                d["rsi"]      = g.get("rsi_14")
                d["bb_mid"]   = g.get("bb_mid")
                d["ema9"]     = g.get("ema9")
                d["ema20"]    = g.get("ema20")
                d["bb_upper"] = g.get("bb_upper")
                d["bb_lower"] = g.get("bb_lower")
                if not d.get("_skip"):
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


def get_bmo_positions() -> list[dict]:
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, symbol, qty, entry_price, entry_date,
                       stop_price, target_price, commission,
                       CURRENT_DATE - entry_date AS holding_days,
                       status, notes
                FROM bmo_positions
                WHERE status = 'open'
                ORDER BY entry_date DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        for r in rows:
            r["entry_date"]    = str(r["entry_date"])
            r["holding_days"]  = int(r["holding_days"])
            r["cost_cad"]      = round(float(r["entry_price"]) * r["qty"] + float(r["commission"]), 2)
            r["entry_price"]   = float(r["entry_price"])
            r["stop_price"]    = float(r["stop_price"] or 0)
            r["target_price"]  = float(r["target_price"] or 0)
        return rows
    except Exception as e:
        log.error(f"BMO持仓查询失败: {e}")
        return []


def add_bmo_position(symbol: str, qty: int, entry_price: float,
                     stop_price: float = None, target_price: float = None,
                     notes: str = "") -> bool:
    try:
        if not stop_price:
            stop_price = round(entry_price * 0.97, 2)
        if not target_price:
            target_price = round(entry_price * 1.15, 2)
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bmo_positions (
                    id SERIAL PRIMARY KEY, symbol TEXT, qty INTEGER,
                    entry_price NUMERIC(10,4), entry_date DATE DEFAULT CURRENT_DATE,
                    stop_price NUMERIC(10,4), target_price NUMERIC(10,4),
                    commission NUMERIC(6,2) DEFAULT 10.0,
                    status TEXT DEFAULT 'open', notes TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO bmo_positions (symbol, qty, entry_price, stop_price, target_price, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (symbol.upper(), qty, entry_price, stop_price, target_price, notes))
        conn.commit()
        conn.close()
        log.info(f"BMO录入: {symbol} {qty}股 @{entry_price}")
        return True
    except Exception as e:
        log.error(f"BMO录入失败: {e}")
        return False


def close_bmo_position(pos_id: int, exit_price: float) -> dict:
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM bmo_positions WHERE id=%s", (pos_id,))
            pos = dict(cur.fetchone())
            gross = (exit_price - float(pos["entry_price"])) * pos["qty"]
            net   = gross - float(pos["commission"]) * 2
            cur.execute("""
                UPDATE bmo_positions SET status='closed',
                notes=CONCAT(notes, ' | 出价:', %s, ' 净盈亏:$', %s)
                WHERE id=%s
            """, (exit_price, round(net,2), pos_id))
        conn.commit()
        conn.close()
        return {"gross": round(gross,2), "net": round(net,2)}
    except Exception as e:
        log.error(f"BMO平仓失败: {e}")
        return {}



def get_node_stats() -> list[dict]:
    """获取各节点实时CPU/内存使用率"""
    import subprocess
    import paramiko

    nodes = [
        {"id": ".18",  "ip": "192.168.0.18",  "name": "Acer XC-605",   "local": True},
        {"id": ".11",  "ip": "192.168.0.11",  "name": "Dell OptiPlex", "local": False},
        {"id": ".143", "ip": "192.168.0.143", "name": "Lenovo",         "local": False},
        {"id": ".101", "ip": "192.168.0.101", "name": "Asus L406M",     "local": False},
        {"id": ".102", "ip": "192.168.0.102", "name": "Asus L410M",     "local": False},
    ]

    CMD = "top -bn1 | grep 'Cpu' | awk '{print $2}' && free -m | awk 'NR==2{printf \"%.0f %.0f\\n\", $3/$2*100, $2}' && systemctl is-active tech-scanner qf-notifier scanner_v4 qf-cad-scanner tsx_scanner signal_fusion ib-executor quant-courier notify-worker quantforce-health 2>/dev/null | paste -d',' - - - - - - - - - - "
    PROCS = {
        ".18":  ["signal_fusion","quantforce-health","qf-panel"],
        ".11":  ["ib-executor","qf-llm-scorer"],
        ".143": ["tech-scanner","scanner_v4","qf-notifier","qf-cad-scanner"],
        ".101": ["grafana-server","quantforce-health"],
        ".102": ["quant-courier","notify-worker"],
    }

    results = []
    for n in nodes:
        try:
            if n["local"]:
                r = subprocess.run(CMD, shell=True, capture_output=True, text=True, timeout=3)
                lines = r.stdout.strip().split('\n')
            else:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(n["ip"], username="heng", timeout=3,
                           key_filename="/home/heng/.ssh/id_rsa")
                _, stdout, _ = ssh.exec_command(CMD, timeout=3)
                lines = stdout.read().decode().strip().split('\n')
                ssh.close()

            cpu_used = float(lines[0]) if lines else 0
            mem_parts = lines[1].split() if len(lines) > 1 else ["0","0"]
            mem_pct = float(mem_parts[0]) if mem_parts else 0
            mem_mb  = int(mem_parts[1]) if len(mem_parts) > 1 else 0

            svc_names = PROCS.get(n["id"], [])
            svc_line = lines[2] if len(lines) > 2 else ""
            svc_statuses = svc_line.split(",") if svc_line else []
            all_svcs = ["tech-scanner","scanner_v4","qf-notifier","qf-cad-scanner","tsx_scanner","signal_fusion","ib-executor","quant-courier","notify-worker","quantforce-health"]
            svc_map = {all_svcs[i]: svc_statuses[i].strip() if i < len(svc_statuses) else "unknown" for i in range(len(all_svcs))}
            procs = [{"name": s, "active": svc_map.get(s,"unknown") == "active"} for s in svc_names]
            results.append({
                **n,
                "alive":   True,
                "cpu_pct": round(cpu_used, 1),
                "mem_pct": round(mem_pct, 1),
                "mem_mb":  mem_mb,
                "procs":   procs,
            })
        except Exception as e:
            results.append({**n, "alive": False, "cpu_pct": 0, "mem_pct": 0, "mem_mb": 0})

    return results

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
        "bmo_positions": get_bmo_positions(),
        "node_stats": get_node_stats(),
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
        elif self.path == "/light":
            self._serve("QuantForce_Apex_v2_light.html")
        elif self.path == "/" or self.path == "/dashboard":
            self._serve("QuantForce_Apex_v2.html")
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

    def _serve(self, filename):
        path = f"/home/heng/quantforce-apex-v2/dashboard/{filename}"
        html = open(path, "rb").read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html)

    def _html(self):
        self._serve("QuantForce_Apex_v2.html")

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5801), PanelHandler)
    log.info("panel_api 启动，监听 0.0.0.0:5801")
    server.serve_forever()
