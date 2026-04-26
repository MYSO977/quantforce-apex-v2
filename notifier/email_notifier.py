#!/usr/bin/env python3
"""
QuantForce Apex v2 — email_notifier.py
节点: .143
功能: 从 PostgreSQL 读取待发送信号，格式化后发送邮件通知
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
        user=PG_USER, password=PG_PASS
    )


def fetch_pending_signals(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, ticker, direction, account, currency,
                   quantity, entry_price, stop_price, target_price,
                   rr_ratio, confidence, source, signal_time,
                   rvol, groq_score, notes
            FROM signals_raw
            WHERE notified = FALSE
              AND confidence >= 7.0
            ORDER BY signal_time ASC
            LIMIT 10
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_notified(conn, signal_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE signals_raw SET notified = TRUE, updated_at = NOW() WHERE id = %s",
            (signal_id,)
        )
    conn.commit()


def format_email(sig):
    direction_icon = "▲ 多头 BUY" if sig["direction"] == "BUY" else "▼ 空头 SELL"
    account_label  = {
        "ib_cash":   "IB 现金账户（USD）",
        "ib_margin": "IB 保证金账户（USD）",
        "bmo_resp":  "BMO RESP（CAD）",
    }.get(sig["account"], sig["account"])

    ep = sig["entry_price"] or 0
    sp = sig["stop_price"]  or 0
    tp = sig["target_price"] or 0
    sl_pct = abs(ep - sp) / ep * 100 if ep else 0
    tp_pct = abs(tp - ep) / ep * 100 if ep else 0

    warnings = []
    if sig["account"] == "ib_cash":
        warnings.append("⚠️  现金账户：T+1结算，请勿当日卖出（GFV规则）")
        warnings.append("⚠️  信号有效期：30分钟内执行")
    elif sig["account"] == "bmo_resp":
        warnings.append("⚠️  BMO RESP：固定手续费$10进+$10出=$20 CAD")
        warnings.append("⚠️  仅限TSX/TSX-V加元计价标的，不可提现")
        warnings.append("⚠️  信号有效期：30分钟内执行")
    elif sig["account"] == "ib_margin":
        warnings.append("⚠️  保证金账户：注意隔夜维持率")
        warnings.append("⚠️  信号有效期：30分钟内执行")

    checks = []
    if sig.get("rvol"):
        checks.append(f"{'✅' if sig['rvol'] >= 1.5 else '❌'} RVOL = {sig['rvol']:.2f}（要求≥1.5）")
    if sig.get("groq_score"):
        checks.append(f"{'✅' if sig['groq_score'] >= 7.5 else '⚠️'} Groq评分 = {sig['groq_score']:.1f}（要求≥7.5）")
    if sig.get("source"):
        checks.append(f"✅ 信号来源：{sig['source']}")

    subject = (
        f"[QF信号] {sig['ticker']} {sig['direction']} | "
        f"{sig['account'].upper()} | "
        f"置信度 {sig['confidence']:.1f}/10"
    )

    body = f"""
════════════════════════════════════════════
📊  QuantForce Labs — 交易信号通知
════════════════════════════════════════════
时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EDT
账户：{account_label}
────────────────────────────────────────────

标的：    {sig['ticker']}（{sig['currency']}）
方向：    {direction_icon}
数量：    {sig['quantity'] or 'XX'} 股
买入价：  {sig['currency']} {ep:.2f}（限价单）
止损价：  {sig['currency']} {sp:.2f}（-{sl_pct:.1f}%）
止盈价：  {sig['currency']} {tp:.2f}（+{tp_pct:.1f}%）
盈亏比：  1 : {sig['rr_ratio'] or 5.0:.1f}
置信度：  {sig['confidence']:.1f} / 10.0

────────────────────────────────────────────
触发条件：
{chr(10).join(checks) if checks else '技术+新闻双重确认'}

────────────────────────────────────────────
下单建议：
  1. 限价买入：{sig['currency']} {ep:.2f}
  2. 止损单：  {sig['currency']} {sp:.2f}（触发后市价卖出）
  3. 止盈单：  {sig['currency']} {tp:.2f}（限价卖出）

────────────────────────────────────────────
注意事项：
{chr(10).join(warnings)}

{('备注：' + sig['notes']) if sig.get('notes') else ''}
════════════════════════════════════════════
QuantForce Labs | 量力实验室 | Winnipeg, CA
Signal ID: {sig['id']}
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


def ensure_notified_column(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE signals_raw
            ADD COLUMN IF NOT EXISTS notified BOOLEAN DEFAULT FALSE
        """)
    conn.commit()


def send_test_email():
    subject = "[QF] email_notifier 启动确认"
    body = (
        f"QuantForce email_notifier 已在 .143 节点启动\n"
        f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"轮询间隔：{POLL_INTERVAL}秒\n"
        f"PG源：{PG_HOST}:{PG_PORT}/{PG_DB}\n\n"
        f"系统正常，等待信号中..."
    )
    send_email(subject, body)
    log.info("测试邮件已发送至 %s", NOTIFY_TO)


def main():
    log.info("email_notifier 启动，轮询间隔 %ds", POLL_INTERVAL)
    try:
        send_test_email()
    except Exception as e:
        log.error("测试邮件发送失败: %s", e)

    while True:
        try:
            conn = get_pg_conn()
            ensure_notified_column(conn)
            signals = fetch_pending_signals(conn)
            if signals:
                log.info("发现 %d 条待发信号", len(signals))
                for sig in signals:
                    try:
                        subject, body = format_email(sig)
                        send_email(subject, body)
                        mark_notified(conn, sig["id"])
                        log.info("✅ 已发送: %s %s (ID:%s)", sig["ticker"], sig["direction"], sig["id"])
                    except Exception as e:
                        log.error("发送失败 ID=%s: %s", sig["id"], e)
            else:
                log.debug("无新信号")
            conn.close()
        except Exception as e:
            log.error("数据库连接失败: %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
