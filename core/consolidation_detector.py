#!/usr/bin/env python3
"""
QuantForce Apex v2 — consolidation_detector.py
横有多长，竖有多高 — 长期横盘突破检测器
节点: .18（被 tech_scanner 和 cad_scanner 调用）

逻辑：
  1. 过去15-60天价格波动 < 8% → 横盘确认
  2. 今日收盘突破横盘上沿 2% → 突破确认
  3. 突破当日 RVOL ≥ 2.0 → 量能确认
  4. 横盘越长评分越高
"""

import logging
import numpy as np
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)

# ─── 参数 ─────────────────────────────────────────────────────
CONSOLIDATION_MIN_DAYS  = 15    # 最短横盘天数
CONSOLIDATION_MAX_DAYS  = 60    # 最长横盘天数
MAX_RANGE_PCT           = 8.0   # 横盘最大波动幅度（%）
BREAKOUT_MIN_PCT        = 2.0   # 突破幅度最小值（%）
BREAKOUT_RVOL_MIN       = 2.0   # 突破时最小RVOL
# ──────────────────────────────────────────────────────────────


def detect_consolidation(ticker: str) -> dict | None:
    """
    检测横盘突破
    返回 None 表示不符合条件
    返回 dict 包含横盘详情和评分
    """
    try:
        tk = yf.Ticker(ticker)

        # 拉取90天日线（足够覆盖最长横盘）
        df = tk.history(period="90d", interval="1d")
        if len(df) < CONSOLIDATION_MIN_DAYS + 5:
            return None

        today_close  = float(df["Close"].iloc[-1])
        today_open   = float(df["Open"].iloc[-1])
        today_volume = float(df["Volume"].iloc[-1])

        # ── 扫描不同横盘时长窗口 ──────────────────────────────
        best = None

        for window in [60, 45, 30, 20, 15]:
            if len(df) < window + 2:
                continue

            # 横盘区间：不含最后1天（今天是突破日）
            consol = df.iloc[-(window + 1):-1]
            high   = float(consol["High"].max())
            low    = float(consol["Low"].min())

            if low <= 0:
                continue

            range_pct = (high - low) / low * 100

            # 横盘条件：波动 < 8%
            if range_pct > MAX_RANGE_PCT:
                continue

            # 突破条件：今日收盘 > 横盘上沿 × 1.02
            breakout_level = high * (1 + BREAKOUT_MIN_PCT / 100)
            if today_close < breakout_level:
                continue

            # 突破幅度
            breakout_pct = (today_close - high) / high * 100

            # RVOL计算
            avg_vol = float(df["Volume"].iloc[-(window + 1):-1].mean())
            rvol = today_volume / avg_vol if avg_vol > 0 else 0

            # 量能确认
            if rvol < BREAKOUT_RVOL_MIN:
                continue

            # 评分（横盘越长越好）
            duration_score = {
                60: 4.0,
                45: 3.0,
                30: 2.0,
                20: 1.5,
                15: 1.0,
            }.get(window, 1.0)

            # 综合评分
            base_score = 6.0
            score = base_score + duration_score
            if rvol >= 3.0:   score += 0.5
            if rvol >= 4.0:   score += 0.5
            if breakout_pct >= 3.0: score += 0.5
            score = min(10.0, score)

            best = {
                "ticker":          ticker,
                "consolidation_days": window,
                "range_pct":       round(range_pct, 2),
                "consol_high":     round(high, 2),
                "consol_low":      round(low, 2),
                "breakout_pct":    round(breakout_pct, 2),
                "breakout_level":  round(breakout_level, 2),
                "today_close":     round(today_close, 2),
                "rvol":            round(rvol, 2),
                "score":           round(score, 1),
                "pattern":         f"{window}天横盘突破",
            }
            break  # 找到最长的横盘窗口就停止

        return best

    except Exception as e:
        log.debug(f"{ticker} 横盘检测失败: {e}")
        return None


def score_description(result: dict) -> str:
    days  = result["consolidation_days"]
    rng   = result["range_pct"]
    brk   = result["breakout_pct"]
    rvol  = result["rvol"]
    score = result["score"]

    quality = "🔥极佳" if score >= 9.0 else "✅良好" if score >= 7.5 else "⚠️一般"
    return (
        f"{quality} {result['ticker']} | "
        f"横盘{days}天(波动{rng:.1f}%) | "
        f"突破{brk:.1f}% | "
        f"RVOL {rvol:.1f} | "
        f"评分{score}"
    )


# ── 命令行测试 ─────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    # 测试几个股票
    test_tickers = ["AAPL", "NVDA", "HIMS", "CLYM", "OKLO", "BE", "IONQ",
                    "RY.TO", "SU.TO", "SHOP.TO", "TD.TO", "CNQ.TO"]

    print("\n=== 横盘突破扫描测试 ===\n")
    found = []
    for t in test_tickers:
        result = detect_consolidation(t)
        if result:
            found.append(result)
            print(score_description(result))
        else:
            print(f"  {t}: 未检测到横盘突破")

    if found:
        print(f"\n共发现 {len(found)} 个横盘突破信号")
        best = max(found, key=lambda x: x["score"])
        print(f"最佳: {score_description(best)}")
    else:
        print("\n当前无横盘突破信号（可能是周末）")
