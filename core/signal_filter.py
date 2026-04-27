#!/usr/bin/env python3
"""
QuantForce Apex v2 — signal_filter.py
三次机会过滤器：只放行真正高质量的信号

过滤逻辑（必须全部满足）：
  L1 综合评分 ≥ 8.0（基础+LLM+GPU平均）
  L2 RVOL ≥ 2.0（不是刚过线的1.5）
  L3 价格 > VWAP（当前价在VWAP之上）
  L4 MACD > 0（动量向上）
  L5 GPU评分 ≥ 8.0（技术面强）
  L6 非重复标的（同一标的24小时内只发一次）
  L7 价格合理（不追涨：当日涨幅 < 15%）

加分项（横盘突破）：
  有 consolidation_days → 直接通过 L1（降低综合分要求至7.5）
  横盘 ≥ 30天 → 额外标记为 🔥 优先信号
"""

import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)

# ─── 过滤参数 ─────────────────────────────────────────────────
MIN_COMPOSITE_SCORE  = 8.0   # 综合评分门槛
MIN_COMPOSITE_CONSOL = 7.5   # 横盘突破时降低门槛
MIN_RVOL             = 2.0   # 最低RVOL
MIN_GPU_SCORE        = 8.0   # GPU技术评分
MAX_DAILY_GAIN_PCT   = 15.0  # 最大当日涨幅（避免追高）
DEDUP_HOURS          = 24    # 同标的去重时间窗口
# ─────────────────────────────────────────────────────────────

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def compute_composite(score, llm_score, gpu_score) -> float:
    scores = [s for s in [score, llm_score, gpu_score] if s is not None]
    return round(sum(scores) / len(scores), 2) if scores else 0.0


def is_duplicate(conn, symbol: str, hours: int = DEDUP_HOURS) -> bool:
    """检查同一标的在时间窗口内是否已发过信号"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM signals_raw
            WHERE symbol = %s
              AND notified = TRUE
              AND created_at > NOW() - INTERVAL '%s hours'
        """, (symbol, hours))
        count = cur.fetchone()[0]
    return count > 0


def filter_signal(sig: dict, conn) -> tuple[bool, str, int]:
    """
    对单条信号做过滤
    返回 (通过/拒绝, 原因, 优先级1-5)
    优先级: 5=🔥极佳 4=✅优秀 3=良好 2=一般 1=勉强
    """
    f         = sig.get("features") or {}
    symbol    = sig.get("symbol", "")
    score     = float(sig.get("score") or 0)
    llm_score = float(sig.get("llm_score") or 0) or None
    gpu_score = float(sig.get("gpu_score") or 0) or None
    rvol      = float(f.get("rvol") or 0)
    vwap      = float(f.get("vwap") or 0)
    price     = float(f.get("price") or 0)
    open_p    = float(f.get("open") or 0)
    macd      = float(f.get("macd") or 0)
    consol    = f.get("consol") or {}
    consol_days = consol.get("consolidation_days", 0) if consol else 0

    composite = compute_composite(score, llm_score, gpu_score)
    is_consol = consol_days >= 15

    # ── L1: 综合评分 ──
    min_score = MIN_COMPOSITE_CONSOL if is_consol else MIN_COMPOSITE_SCORE
    if composite < min_score:
        return False, f"综合评分不足 {composite:.1f} < {min_score}", 0

    # ── L2: RVOL ──
    if rvol < MIN_RVOL:
        return False, f"RVOL不足 {rvol:.2f} < {MIN_RVOL}", 0

    # ── L3: 价格 > VWAP ──
    if vwap > 0 and price <= vwap:
        return False, f"价格({price}) ≤ VWAP({vwap})", 0

    # ── L4: MACD ──
    if macd <= 0:
        return False, f"MACD({macd:.4f}) ≤ 0", 0

    # ── L5: GPU评分 ──
    if gpu_score is not None and gpu_score < MIN_GPU_SCORE:
        return False, f"GPU评分不足 {gpu_score:.1f} < {MIN_GPU_SCORE}", 0

    # ── L6: 去重 ──
    if is_duplicate(conn, symbol):
        return False, f"{symbol} 24小时内已发过信号", 0

    # ── L7: 避免追高 ──
    if open_p > 0 and price > 0:
        daily_gain = (price - open_p) / open_p * 100
        if daily_gain > MAX_DAILY_GAIN_PCT:
            return False, f"当日涨幅过高 {daily_gain:.1f}% > {MAX_DAILY_GAIN_PCT}%", 0

    # ── 优先级评定 ──
    priority = 3  # 默认良好
    if is_consol and consol_days >= 30:
        priority = 5  # 🔥 长期横盘突破
    elif is_consol:
        priority = 4  # ✅ 横盘突破
    elif composite >= 9.0:
        priority = 4  # ✅ 高分信号
    elif rvol >= 3.0 and composite >= 8.5:
        priority = 4

    reason = f"通过 综合{composite:.1f} RVOL{rvol:.1f}"
    if is_consol:
        reason += f" 横盘{consol_days}天突破"

    return True, reason, priority


def run_filter(limit: int = 20) -> list[dict]:
    """
    扫描待发信号，过滤并按优先级排序
    返回通过过滤的信号列表
    """
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, signal_id, symbol, direction,
                       score, llm_score, gpu_score,
                       source, features, created_at
                FROM signals_raw
                WHERE notified = FALSE
                  AND status = 'pending'
                  AND direction = 'buy'
                  AND confidence >= 7.0
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            candidates = [dict(r) for r in cur.fetchall()]

        passed = []
        for sig in candidates:
            ok, reason, priority = filter_signal(sig, conn)
            if ok:
                sig["_priority"] = priority
                sig["_reason"]   = reason
                passed.append(sig)
                log.info(f"✅ 通过[P{priority}] {sig['symbol']} — {reason}")
            else:
                log.debug(f"❌ 过滤 {sig['symbol']} — {reason}")
                # 标记为已通知（避免重复检查）
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE signals_raw SET notified=TRUE WHERE id=%s",
                        (sig["id"],)
                    )
                conn.commit()

        # 按优先级排序
        passed.sort(key=lambda x: x["_priority"], reverse=True)
        return passed

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    results = run_filter()
    print(f"\n过滤结果: {len(results)} 条通过")
    for r in results:
        print(f"  P{r['_priority']} {r['symbol']} — {r['_reason']}")
