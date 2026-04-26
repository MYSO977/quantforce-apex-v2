#!/usr/bin/env python3
"""
QuantForce Apex v2 — gpu_indicator.py
节点: .11 (GTX 750 / CUDA 12.2)
功能: GPU批量计算技术指标，对 signals_raw 中 pending 信号做二次验证
      计算多周期 EMA、MACD、RSI、布林带，更新 features 字段
"""

import time
import logging
import psycopg2
import psycopg2.extras
import yfinance as yf
import torch
import json
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"

POLL_INTERVAL = 20
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GPU_IND] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE signals_raw
            ADD COLUMN IF NOT EXISTS gpu_score NUMERIC(4,2),
            ADD COLUMN IF NOT EXISTS gpu_indicators JSONB
        """)
    conn.commit()


def fetch_unprocessed(conn, limit=10) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, symbol, direction, score, features
            FROM signals_raw
            WHERE gpu_score IS NULL
              AND status = 'pending'
              AND direction = 'buy'
            ORDER BY id ASC
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]


# ── GPU 指标计算 ──────────────────────────────────────────────
def gpu_ema(prices: torch.Tensor, span: int) -> torch.Tensor:
    """GPU上计算EMA"""
    alpha = 2.0 / (span + 1)
    ema = prices.clone()
    for i in range(1, len(prices)):
        ema[i] = alpha * prices[i] + (1 - alpha) * ema[i - 1]
    return ema


def gpu_rsi(prices: torch.Tensor, period: int = 14) -> float:
    """GPU上计算RSI"""
    deltas = prices[1:] - prices[:-1]
    gains  = torch.clamp(deltas, min=0)
    losses = torch.clamp(-deltas, min=0)
    avg_gain = gains[-period:].mean()
    avg_loss = losses[-period:].mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))


def compute_gpu_indicators(symbol: str) -> dict | None:
    try:
        tk   = yf.Ticker(symbol)
        df5  = tk.history(period="5d", interval="5m")
        df1d = tk.history(period="30d", interval="1d")

        if df5.empty or len(df5) < 30:
            return None

        # 转为 GPU tensor
        closes5 = torch.tensor(df5["Close"].values, dtype=torch.float32, device=DEVICE)
        closes1d = torch.tensor(df1d["Close"].values, dtype=torch.float32, device=DEVICE) if len(df1d) >= 14 else None

        # EMA
        ema9  = gpu_ema(closes5, 9)
        ema20 = gpu_ema(closes5, 20)
        ema50 = gpu_ema(closes5, 50) if len(closes5) >= 50 else None

        # MACD
        ema12 = gpu_ema(closes5, 12)
        ema26 = gpu_ema(closes5, 26)
        macd_line   = float((ema12 - ema26)[-1])
        signal_line = float(gpu_ema(ema12 - ema26, 9)[-1])
        macd_hist   = macd_line - signal_line

        # RSI (日线)
        rsi = gpu_rsi(closes1d, 14) if closes1d is not None and len(closes1d) >= 15 else 50.0

        # 布林带 (5分钟，20周期)
        window = closes5[-20:]
        bb_mid  = float(window.mean())
        bb_std  = float(window.std())
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        current  = float(closes5[-1])

        # EMA趋势
        ema9_slope  = float(ema9[-1] - ema9[-3]) / float(ema9[-3]) * 100
        ema20_slope = float(ema20[-1] - ema20[-3]) / float(ema20[-3]) * 100
        ema9_above_ema20 = float(ema9[-1]) > float(ema20[-1])

        # GPU评分
        gpu_score = 5.0
        if macd_line > 0:           gpu_score += 1.5
        if macd_hist > 0:           gpu_score += 0.5
        if ema9_above_ema20:        gpu_score += 1.0
        if ema9_slope > 0:          gpu_score += 0.5
        if 40 < rsi < 70:           gpu_score += 0.5
        if current > bb_mid:        gpu_score += 0.5
        if current < bb_upper:      gpu_score += 0.5
        gpu_score = min(10.0, gpu_score)

        return {
            "ema9":          round(float(ema9[-1]), 4),
            "ema20":         round(float(ema20[-1]), 4),
            "ema50":         round(float(ema50[-1]), 4) if ema50 is not None else None,
            "macd_line":     round(macd_line, 4),
            "macd_signal":   round(signal_line, 4),
            "macd_hist":     round(macd_hist, 4),
            "rsi_14":        round(rsi, 1),
            "bb_upper":      round(bb_upper, 2),
            "bb_mid":        round(bb_mid, 2),
            "bb_lower":      round(bb_lower, 2),
            "ema9_slope":    round(ema9_slope, 4),
            "ema9_above_20": ema9_above_ema20,
            "gpu_score":     round(gpu_score, 2),
            "device":        str(DEVICE),
        }
    except Exception as e:
        log.debug(f"{symbol} GPU计算失败: {e}")
        return None


def update_signal(conn, row_id: int, gpu_score: float, indicators: dict):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE signals_raw
            SET gpu_score = %s,
                gpu_indicators = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (gpu_score, psycopg2.extras.Json(indicators), row_id))
    conn.commit()


def main():
    log.info(f"gpu_indicator 启动 | 设备: {DEVICE}")
    log.info(f"GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CPU'}")

    conn = get_pg_conn()
    ensure_columns(conn)
    conn.close()

    while True:
        try:
            conn = get_pg_conn()
            signals = fetch_unprocessed(conn)

            if signals:
                log.info(f"待GPU计算: {len(signals)} 条")
                for sig in signals:
                    t0 = time.time()
                    indicators = compute_gpu_indicators(sig["symbol"])
                    if indicators:
                        gpu_score = indicators.pop("gpu_score")
                        update_signal(conn, sig["id"], gpu_score, indicators)
                        elapsed = round(time.time() - t0, 2)
                        log.info(f"✅ {sig['symbol']} GPU评分:{gpu_score:.1f} "
                                f"RSI:{indicators.get('rsi_14')} "
                                f"MACD:{indicators.get('macd_line')} "
                                f"耗时:{elapsed}s")
                    else:
                        log.warning(f"⚠️ {sig['symbol']} 数据不足，跳过")
            else:
                log.debug("无待处理信号")

            conn.close()
        except Exception as e:
            log.error(f"错误: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
