#!/usr/bin/env python3
"""
QuantForce Apex v2 — email_notifier.py
节点: .143
"""

import smtplib
import psycopg2
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
        user=PG_USER, password=PG_PASS,
        connect_timeout=5
    )


def fetch_pending_signals(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, signal_id, symbol, direction, confidence,
                   score, source, pipeline, features, expire_at
            FROM signals_raw
            WHERE notified = FALSE
              AND confidence >= 7.0
              AND status = 'pending'
            ORDER BY id ASC
            LIMIT 10
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


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

    sl_pct = 3.0
    tp_pct = sl_pct * 5
    if direction == "buy":
        stop_price   = round(price * (1 - sl_pct / 100), 2)
        target_price = round(price * (1 + tp_pct / 100), 2)
    else:
        stop_price   = round(price * (1 + sl_pct / 100), 2)
        target_price = round(price * (1 - tp_pct / 100), 2)

    direction_icon = "▲ 多头 BUY" if direction == "buy" else "▼ 空头 SELL"

    checks = []
    if rvol:
        checks.append(f"{'✅' if rvol >= 1.5 else '❌'} RVOL = {rvol:.2f}（要求≥1.5）")
    if vwap and price:
        checks.append(f"{'✅' if price > vwap else '❌'} Price {price} > VWAP {vwap}")
    if macd:
        checks.append(f"{'✅' if macd > 0 else '❌'} MACD = {macd:.4f}")
    if ema9:
        checks.append(f"✅ EMA9 = {ema9:.4f}")
    if score:
        checks.append(f"{'✅' if score >= 7.5 else '⚠️'} 综合评分 = {score:.1f}")
    checks.append(f"✅ 来源：{source}")

    subject = f"[QF信号] {symbol} {direction.upper()} | 评分 {score:.1f} | 置信度 {sig['confidence']:.1f}/10"

    body = f"""
════════════════════════════════════════════
📊  QuantForce Labs — 交易信号通知
════════════════════════════════════════════
时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EDT
信号ID：{sig['signal_id']}
────────────────────────────────────────────
标的：    {symbol}
方向：    {direction_icon}
当前价：  $ {price:.2f}
止损价：  $ {stop_price}（-{sl_pct:.0f}%）
止盈价：  $ {target_price}（+{tp_pct:.0f}%）
盈亏比：  1 : 5.0
置信度：  {sig['confidence']:.1f} / 10.0
综合评分：{score:.1f} / 10.0
────────────────────────────────────────────
触发条件：
{chr(10).join(checks)}
────────────────────────────────────────────
下单建议（IB USD 现金账户）：
  仓位：  $400 USD
  限价买：$ {price:.2f}
  止损：  $ {stop_price}
  止盈：  $ {target_price}
────────────────────────────────────────────
⚠️  现金账户：T+1结算，请勿当日卖出（GFV）
⚠️  信号有效期：30分钟内执行
════════════════════════════════════════════
QuantForce Labs | 量力实验室 | Winnipeg, CA
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
    log.info("email_notifier v2 启动")
    try:
        send_email("[QF] email_notifier 启动确认 v3",
                   f"已启动，轮询间隔{POLL_INTERVAL}秒，等待信号中...")
        log.info("测试邮件已发送")
    except Exception as e:
        log.error("测试邮件失败: %s", e)

    while True:
        try:
            log.info("轮询中...")
            conn = get_pg_conn()
            signals = fetch_pending_signals(conn)
            if signals:
                log.info("发现 %d 条待发信号", len(signals))
                for sig in signals:
                    try:
                        subject, body = format_email(sig)
                        send_email(subject, body)
                        mark_notified(conn, sig["id"])
                        log.info("✅ 已发送: %s %s", sig["symbol"], sig["direction"])
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
