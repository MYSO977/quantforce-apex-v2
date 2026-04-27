#!/usr/bin/env python3
"""
QuantForce Apex v2 — email_notifier.py v4
节点: .143
三项评分：基础score + LLM量价 + GPU技术指标
"""

import smtplib
import psycopg2
import psycopg2.extras
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

GMAIL_USER     = "wuheng998@gmail.com"
GMAIL_APP_PASS = "bxnpkiujvokqswlz"
NOTIFY_TO      = "wuheng998@gmail.com"

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"

POLL_INTERVAL = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NOTIFIER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def fetch_pending_signals(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, signal_id, symbol, direction, confidence,
                   score, source, pipeline, features, expire_at,
                   llm_score, llm_reason, gpu_score, gpu_indicators
            FROM signals_raw
            WHERE notified = FALSE
              AND confidence >= 7.0
              AND status = 'pending'
              AND direction = 'buy'
            ORDER BY id ASC
            LIMIT 5
        """)
        return [dict(r) for r in cur.fetchall()]


def mark_notified(conn, row_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE signals_raw SET notified = TRUE WHERE id = %s", (row_id,))
    conn.commit()


def format_email(sig):
    f   = sig["features"] or {}
    gi  = sig["gpu_indicators"] or {}

    symbol    = sig["symbol"]
    price     = f.get("price", 0)
    rvol      = f.get("rvol", 0)
    vwap      = f.get("vwap", 0)
    macd      = f.get("macd", 0)
    ema9      = f.get("ema9")
    score     = f.get("score", sig["score"])
    source    = f.get("source", sig["source"])
    currency  = f.get("currency", "USD")
    account   = f.get("account", "ib_cash")

    llm_score  = float(sig["llm_score"]) if sig["llm_score"] else None
    llm_reason = sig.get("llm_reason", "")
    gpu_score  = float(sig["gpu_score"]) if sig["gpu_score"] else None

    # GPU指标
    rsi      = gi.get("rsi_14")
    macd_gpu = gi.get("macd_line")
    bb_mid   = gi.get("bb_mid")
    ema9_gpu = gi.get("ema9")
    ema20    = gi.get("ema20")

    # 账户标签和仓位
    account_label = {
        "ib_cash":   "IB 现金账户（USD）",
        "ib_margin": "IB 保证金账户（USD）",
        "bmo_resp":  "BMO RESP（CAD）",
    }.get(account, account)
    qty      = f.get("qty", 0)
    cost     = f.get("cost", 0)
    pos_usd  = f.get("position", 400 if "ib" in account else 3000)
    if qty:
        position = f"{qty}股 × {currency}{price:.2f} = {currency}{cost:.2f}"
    else:
        qty = max(1, int((400 if "ib" in account else 3000) / price)) if price > 0 else 1
        position = f"{qty}股 × {currency}{price:.2f} = {currency}{round(qty*price,2):.2f}"

    # 止损止盈
    sl_pct = 3.0
    tp_pct = sl_pct * 5
    stop_price   = round(price * (1 - sl_pct / 100), 2)
    target_price = round(price * (1 + tp_pct / 100), 2)

    # 综合评分
    scores = [s for s in [score, llm_score, gpu_score] if s is not None]
    composite = round(sum(scores) / len(scores), 1)

    # 评分栏
    score_lines = []
    score_lines.append(f"  基础评分：  {score:.1f} / 10.0（信号触发）")
    if llm_score is not None:
        score_lines.append(f"  LLM评分：   {llm_score:.1f} / 10.0（qwen2.5量价）")
        if llm_reason:
            score_lines.append(f"              {llm_reason[:60]}")
    if gpu_score is not None:
        score_lines.append(f"  GPU评分：   {gpu_score:.1f} / 10.0（GTX750技术指标）")
    score_lines.append(f"  ─────────────────────────────")
    score_lines.append(f"  综合评分：  {composite:.1f} / 10.0")

    # GPU指标栏
    gpu_lines = []
    if rsi:
        rsi_ok = 30 < rsi < 75
        gpu_lines.append(f"  {'✅' if rsi_ok else '⚠️'} RSI(14) = {rsi:.1f}")
    if macd_gpu:
        gpu_lines.append(f"  {'✅' if macd_gpu > 0 else '❌'} MACD = {macd_gpu:.4f}")
    if ema9_gpu and ema20:
        gpu_lines.append(f"  {'✅' if ema9_gpu > ema20 else '❌'} EMA9({ema9_gpu:.2f}) {'>' if ema9_gpu > ema20 else '<'} EMA20({ema20:.2f})")
    if price and bb_mid:
        gpu_lines.append(f"  {'✅' if price > bb_mid else '❌'} Price({price}) {'>' if price > bb_mid else '<'} BB_Mid({bb_mid:.2f})")

    # 基础条件
    basic_lines = []
    if rvol:
        basic_lines.append(f"  {'✅' if rvol >= 1.5 else '❌'} RVOL = {rvol:.2f}")
    if vwap and price:
        basic_lines.append(f"  {'✅' if price > vwap else '❌'} Price > VWAP ({vwap:.2f})")
    if macd:
        basic_lines.append(f"  {'✅' if macd > 0 else '❌'} MACD = {macd:.4f}")

    # 警告
    warnings = []
    if account == "ib_cash":
        warnings.append("⚠️  T+1结算，请勿当日卖出（GFV）")
        warnings.append("⚠️  信号有效期30分钟")
    elif account == "bmo_resp":
        warnings.append("⚠️  手续费$10进+$10出=$20 CAD")
        warnings.append("⚠️  仅限TSX/TSX-V加元标的")
        warnings.append("⚠️  信号有效期30分钟")

    subject = f"[QF{composite:.0f}分] {symbol} BUY | {account_label[:8]} | {currency}{price:.2f}"

    body = f"""
════════════════════════════════════════════
📊  QuantForce Labs — 交易信号通知
════════════════════════════════════════════
时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EDT
账户：{account_label}
────────────────────────────────────────────
标的：    {symbol}（{currency}）
方向：    ▲ 多头 BUY
当前价：  {currency} {price:.2f}
止损价：  {currency} {stop_price}（-{sl_pct:.0f}%）
止盈价：  {currency} {target_price}（+{tp_pct:.0f}%）
盈亏比：  1 : 5.0
仓位：    {position}
────────────────────────────────────────────
📊 评分详情：
{chr(10).join(score_lines)}
────────────────────────────────────────────
🖥️  GPU技术指标（.11 GTX750）：
{chr(10).join(gpu_lines) if gpu_lines else '  计算中...'}
────────────────────────────────────────────
📡 基础触发条件：
{chr(10).join(basic_lines)}
────────────────────────────────────────────
下单建议：
  1. 限价买入：{currency} {price:.2f}
  2. 止损单：  {currency} {stop_price}
  3. 止盈单：  {currency} {target_price}
────────────────────────────────────────────
{chr(10).join(warnings)}
════════════════════════════════════════════
QuantForce Labs | 量力实验室 | Winnipeg, CA
来源：{source} | ID：{sig['signal_id'][:8]}
════════════════════════════════════════════
""".strip()

    return subject, body


def send_email(subject, body):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = NOTIFY_TO
    msg.attach(MIMEText(body, "plain", "utf-8"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASS)
        server.sendmail(GMAIL_USER, NOTIFY_TO, msg.as_string())


def main():
    log.info("email_notifier v4 启动（三项评分）")
    try:
        send_email("[QF] notifier v4 启动",
                   f"三项评分版已启动\n基础+LLM+GPU\n{datetime.now()}")
        log.info("测试邮件已发送")
    except Exception as e:
        log.error("测试邮件失败: %s", e)

    while True:
        try:
            conn = get_pg_conn()
            signals = fetch_pending_signals(conn)
            if signals:
                log.info("发现 %d 条待发信号", len(signals))
                for sig in signals:
                    try:
                        subject, body = format_email(sig)
                        send_email(subject, body)
                        mark_notified(conn, sig["id"])
                        log.info("✅ 已发送: %s 综合%.1f LLM%.1f GPU%.1f",
                                 sig["symbol"],
                                 float(sig["score"]),
                                 float(sig["llm_score"]) if sig["llm_score"] else 0,
                                 float(sig["gpu_score"]) if sig["gpu_score"] else 0)
                    except Exception as e:
                        log.error("发送失败: %s", e)
            else:
                log.info("无新信号")
            conn.close()
        except Exception as e:
            log.error("DB错误: %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
