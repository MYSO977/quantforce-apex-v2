#!/usr/bin/env python3
"""
QuantForce Apex v2 — email_notifier.py
节点: .143
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
                   llm_score, llm_reason
            FROM signals_raw
            WHERE notified = FALSE
              AND confidence >= 7.0
              AND status = 'pending'
            ORDER BY id ASC
            LIMIT 10
        """)
        return [dict(r) for r in cur.fetchall()]


def mark_notified(conn, row_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE signals_raw SET notified = TRUE WHERE id = %s",
            (row_id,)
        )
    conn.commit()


def format_email(sig):
    f = sig["features"] or {}
    symbol    = sig["symbol"]
    direction = sig["direction"]
    price     = f.get("price", 0)
    rvol      = f.get("rvol", 0)
    vwap      = f.get("vwap", 0)
    macd      = f.get("macd", 0)
    ema9      = f.get("ema9")
    score     = f.get("score", sig["score"])
    source    = f.get("source", sig["source"])
    currency  = f.get("currency", "USD")
    account   = f.get("account", "ib_cash")
    llm_score = sig.get("llm_score")
    llm_reason= sig.get("llm_reason", "")

    # 账户标签
    account_label = {
        "ib_cash":   "IB 现金账户（USD）",
        "ib_margin": "IB 保证金账户（USD）",
        "bmo_resp":  "BMO RESP（CAD）",
    }.get(account, account)

    # 止损止盈估算
    sl_pct = 3.0
    tp_pct = sl_pct * 5
    if direction == "buy":
        stop_price   = round(price * (1 - sl_pct / 100), 2)
        target_price = round(price * (1 + tp_pct / 100), 2)
        direction_icon = "▲ 多头 BUY"
    else:
        stop_price   = round(price * (1 + sl_pct / 100), 2)
        target_price = round(price * (1 - tp_pct / 100), 2)
        direction_icon = "▼ 空头 SELL"

    # 仓位
    position = "$400 USD" if "ib" in account else "$3,000 CAD"

    # 触发条件
    checks = []
    if rvol:
        checks.append(f"{'✅' if rvol >= 1.5 else '❌'} RVOL = {rvol:.2f}（要求≥1.5）")
    if vwap and price:
        checks.append(f"{'✅' if price > vwap else '❌'} Price {price} > VWAP {vwap}")
    if macd:
        checks.append(f"{'✅' if macd > 0 else '❌'} MACD = {macd:.4f}")
    if ema9:
        checks.append(f"✅ EMA9 = {ema9:.4f}")
    checks.append(f"✅ 基础评分 = {score:.1f} / 10.0")

    # LLM评分
    llm_line = ""
    if llm_score is not None:
        llm_ok = float(llm_score) >= 7.0
        llm_line = f"\n{'✅' if llm_ok else '⚠️'} LLM量价评分 = {float(llm_score):.1f} / 10.0"
        if llm_reason:
            llm_line += f"\n   理由：{llm_reason}"

    # 账户警告
    warnings = []
    if account == "ib_cash":
        warnings.append("⚠️  现金账户：T+1结算，请勿当日卖出（GFV）")
        warnings.append("⚠️  信号有效期：30分钟内执行")
    elif account == "bmo_resp":
        warnings.append("⚠️  BMO RESP：固定手续费$10进+$10出=$20 CAD")
        warnings.append("⚠️  仅限TSX/TSX-V加元标的，不可提现")
        warnings.append("⚠️  信号有效期：30分钟内执行")

    # 综合评分（基础 + LLM 加权）
    if llm_score is not None:
        composite = round((score + float(llm_score)) / 2, 1)
        composite_line = f"综合评分：{composite:.1f} / 10.0（基础{score:.1f} + LLM{float(llm_score):.1f}）"
    else:
        composite_line = f"综合评分：{score:.1f} / 10.0"

    subject = (
        f"[QF信号] {symbol} {direction.upper()} | "
        f"{composite_line[:20]} | "
        f"账户:{account.upper()}"
    )

    body = f"""
════════════════════════════════════════════
📊  QuantForce Labs — 交易信号通知
════════════════════════════════════════════
时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EDT
账户：{account_label}
信号ID：{sig['signal_id']}
────────────────────────────────────────────
标的：    {symbol}（{currency}）
方向：    {direction_icon}
当前价：  {currency} {price:.2f}
止损价：  {currency} {stop_price}（-{sl_pct:.0f}%，估算）
止盈价：  {currency} {target_price}（+{tp_pct:.0f}%，估算）
盈亏比：  1 : 5.0
仓位：    {position}
────────────────────────────────────────────
评分详情：
{chr(10).join(checks)}{llm_line}
{composite_line}
────────────────────────────────────────────
下单建议：
  1. 限价买入：{currency} {price:.2f}
  2. 止损单：  {currency} {stop_price}
  3. 止盈单：  {currency} {target_price}
────────────────────────────────────────────
{chr(10).join(warnings)}
════════════════════════════════════════════
QuantForce Labs | 量力实验室 | Winnipeg, CA
来源：{source}
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
    log.info("email_notifier v3 启动（含LLM评分）")
    try:
        send_email("[QF] email_notifier v3 启动确认",
                   f"notifier v3 已启动\n含LLM量价评分字段\n时间：{datetime.now()}")
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
                        log.info("✅ 已发送: %s %s llm=%.1f",
                                 sig["symbol"], sig["direction"],
                                 float(sig["llm_score"]) if sig["llm_score"] else 0)
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
