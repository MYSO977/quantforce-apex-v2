#!/usr/bin/env python3
"""
QuantForce Apex v2 — llm_scorer.py
用本地 qwen2.5:0.5b 对量价形态做二次评分
.11 主力，.18 备用
轮询 signals_raw 中 llm_score IS NULL 的信号，打分后更新
"""

import time
import logging
import psycopg2
import psycopg2.extras
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ─── CONFIG ──────────────────────────────────────────────────
# .11 主力，.18 备用
OLLAMA_HOSTS = [
    "http://192.168.0.11:11434",
    "http://192.168.0.18:11434",
]
OLLAMA_MODEL  = "qwen2.5:0.5b"
POLL_INTERVAL = 20   # 秒

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LLM_SCORER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def ensure_llm_score_column(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE signals_raw
            ADD COLUMN IF NOT EXISTS llm_score NUMERIC(4,2)
        """)
        cur.execute("""
            ALTER TABLE signals_raw
            ADD COLUMN IF NOT EXISTS llm_reason TEXT
        """)
    conn.commit()


def fetch_unscored(conn, limit=5) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, symbol, direction, confidence, score, features
            FROM signals_raw
            WHERE llm_score IS NULL
              AND status = 'pending'
            ORDER BY id ASC
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]


def update_llm_score(conn, row_id: int, llm_score: float, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE signals_raw
            SET llm_score = %s,
                llm_reason = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (llm_score, reason[:500], row_id))
    conn.commit()


def build_prompt(sig: dict) -> str:
    f = sig["features"] or {}
    price = f.get("price", 0)
    rvol  = f.get("rvol", 0)
    vwap  = f.get("vwap", 0)
    macd  = f.get("macd", 0)
    ema9  = f.get("ema9", 0)
    open_ = f.get("open", 0)

    above_vwap = "yes" if price > vwap else "no"
    above_open = "yes" if price > open_ else "no"
    macd_pos   = "yes" if macd > 0 else "no"
    ema9_val   = f"{ema9:.4f}" if ema9 else "N/A"

    return f"""You are a quantitative trading signal evaluator.
Evaluate this technical signal and give a score from 0 to 10.

Symbol: {sig['symbol']}
Direction: {sig['direction'].upper()}
Price: {price}
RVOL: {rvol} (above 1.5 is bullish)
Price above VWAP: {above_vwap}
Price above open: {above_open}
MACD positive: {macd_pos} (value: {macd})
EMA9: {ema9_val}
Base score: {sig['score']}

Scoring criteria:
- RVOL >= 2.0: +2 points
- RVOL >= 1.5: +1 point
- Price > VWAP: +2 points
- MACD > 0: +2 points
- Price > open: +1 point
- EMA9 trending up: +1 point
- Strong momentum combination: +1 bonus

Reply in this exact JSON format only, no other text:
{{"score": 7.5, "reason": "Strong RVOL with price above VWAP and positive MACD"}}"""


def call_ollama(prompt: str) -> tuple[float, str]:
    """调用 Ollama，主力 .11，失败切换 .18"""
    for host in OLLAMA_HOSTS:
        try:
            resp = requests.post(
                f"{host}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 100}
                },
                timeout=15
            )
            text = resp.json().get("response", "").strip()
            # 提取 JSON
            start = text.find("{")
            end   = text.rfind("}") + 1
            if start >= 0 and end > start:
                data   = json.loads(text[start:end])
                score  = float(data.get("score", 5.0))
                reason = str(data.get("reason", ""))
                score  = max(0.0, min(10.0, score))
                log.debug(f"Ollama [{host}] 评分: {score} — {reason}")
                return score, reason
        except Exception as e:
            log.warning(f"Ollama [{host}] 失败: {e}")
            continue

    log.error("所有 Ollama 节点均不可用，使用默认分")
    return 5.0, "ollama unavailable"


def main():
    log.info("llm_scorer 启动（.11主力 / .18备用）")

    conn = get_pg_conn()
    ensure_llm_score_column(conn)
    conn.close()

    while True:
        try:
            conn = get_pg_conn()
            signals = fetch_unscored(conn)

            if signals:
                log.info(f"待评分信号: {len(signals)} 条")
                for sig in signals:
                    prompt = build_prompt(sig)
                    llm_score, reason = call_ollama(prompt)
                    update_llm_score(conn, sig["id"], llm_score, reason)
                    log.info(f"✅ {sig['symbol']} LLM评分: {llm_score:.1f} — {reason[:60]}")
            else:
                log.debug("无待评分信号")

            conn.close()

        except Exception as e:
            log.error(f"错误: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
