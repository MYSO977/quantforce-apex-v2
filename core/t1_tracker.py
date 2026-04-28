#!/usr/bin/env python3
"""
QuantForce Apex v2 — t1_tracker.py
IB 现金账户 T+1 资金追踪器，防止 GFV（Good Faith Violation）

规则：
  - 现金账户当日买入可以当日卖出
  - 卖出后的资金需等 T+1 结算才能再次买入
  - 最大同时持仓 3 笔（$400 × 3 = $1,200）

用法：
  tracker = T1Tracker()
  ok, reason = tracker.can_buy("ACMR", 400.0)
  if ok:
      tracker.record_buy("ACMR", 400.0)
"""

import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────
ACCOUNT_BALANCE  = 1200.0   # USD 总资金
MAX_POSITION_USD = 400.0    # 单笔最大仓位
MAX_CONCURRENT   = 3        # 最大同时持仓数

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"
# ─────────────────────────────────────────────────────────────


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        connect_timeout=5
    )


def ensure_tables():
    """确保 t1_positions 表存在"""
    conn = get_pg_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS t1_positions (
                id            SERIAL PRIMARY KEY,
                symbol        TEXT NOT NULL,
                account       TEXT NOT NULL DEFAULT 'ib_cash',
                buy_date      DATE NOT NULL,
                settle_date   DATE NOT NULL,   -- T+1 结算日
                amount_usd    NUMERIC(10,2) NOT NULL,
                qty           INTEGER NOT NULL DEFAULT 0,
                entry_price   NUMERIC(10,4) NOT NULL DEFAULT 0,
                status        TEXT NOT NULL DEFAULT 'open',  -- open / closed
                sell_date     DATE,
                sell_price    NUMERIC(10,4),
                pnl_usd       NUMERIC(10,2),
                created_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_t1_status
            ON t1_positions(account, status)
        """)
    conn.commit()
    conn.close()
    log.info("t1_positions 表已就绪")


class T1Tracker:
    def __init__(self, account: str = "ib_cash"):
        self.account = account
        ensure_tables()

    def _get_conn(self):
        return get_pg_conn()

    def _next_business_day(self, d: date) -> date:
        """获取下一个交易日（跳过周末）"""
        next_d = d + timedelta(days=1)
        while next_d.weekday() >= 5:  # 5=周六, 6=周日
            next_d += timedelta(days=1)
        return next_d

    def get_open_positions(self) -> list[dict]:
        """获取所有未平仓位"""
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM t1_positions
                WHERE account = %s AND status = 'open'
                ORDER BY buy_date ASC
            """, (self.account,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_settled_cash(self) -> float:
        """
        计算已结算的可用资金
        已结算 = 总资金 - 所有 open 仓位占用资金
        """
        positions = self.get_open_positions()
        used = sum(float(p["amount_usd"]) for p in positions)
        available = ACCOUNT_BALANCE - used
        return max(available, 0.0)

    def get_unsettled_sells(self) -> list[dict]:
        """
        获取已卖出但未结算的仓位（sell_date = 今天，settle_date > 今天）
        这些资金不能立即再用
        """
        today = datetime.now(ET).date()
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM t1_positions
                WHERE account = %s
                  AND status = 'closed'
                  AND sell_date = %s
                  AND settle_date > %s
            """, (self.account, today, today))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def can_buy(self, symbol: str, amount_usd: float) -> tuple[bool, str]:
        """
        检查是否可以买入
        返回 (True/False, 原因说明)
        """
        today = datetime.now(ET).date()
        positions = self.get_open_positions()

        # ── 规则1: 最大持仓数 ──
        if len(positions) >= MAX_CONCURRENT:
            return False, f"已达最大持仓数 {MAX_CONCURRENT} 笔，当前 {len(positions)} 笔开仓"

        # ── 规则2: 单笔仓位上限 ──
        if amount_usd > MAX_POSITION_USD:
            return False, f"单笔仓位 ${amount_usd} 超过上限 ${MAX_POSITION_USD}"

        # ── 规则3: 可用资金检查 ──
        available = self.get_settled_cash()
        if amount_usd > available:
            return False, f"可用资金不足：需要 ${amount_usd}，可用 ${available:.2f}"

        # ── 规则4: GFV 防护 ──
        # 当日卖出的资金（未结算）不能立即再买入
        unsettled = self.get_unsettled_sells()
        unsettled_amount = sum(float(p["amount_usd"]) for p in unsettled)
        if unsettled_amount > 0:
            # 检查可用资金是否依赖未结算资金
            truly_available = available - unsettled_amount
            if amount_usd > truly_available:
                settle_dates = [str(p["settle_date"]) for p in unsettled]
                return False, (
                    f"GFV风险：当日卖出 ${unsettled_amount:.2f} 未结算（结算日: {settle_dates}），"
                    f"真实可用资金 ${truly_available:.2f}"
                )

        # ── 规则5: 同一标的当日已买入检查 ──
        today_buys = [p for p in positions if p["symbol"] == symbol and p["buy_date"] == today]
        if today_buys:
            return False, f"{symbol} 今日已有买入仓位，现金账户避免同日重复买入"

        return True, f"✅ 可以买入 {symbol} ${amount_usd}，可用资金 ${available:.2f}"

    def record_buy(self, symbol: str, amount_usd: float,
                   qty: int = 0, entry_price: float = 0.0) -> int:
        """记录买入，返回仓位ID"""
        today       = datetime.now(ET).date()
        settle_date = self._next_business_day(today)  # T+1

        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO t1_positions
                  (symbol, account, buy_date, settle_date, amount_usd, qty, entry_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (symbol, self.account, today, settle_date,
                  amount_usd, qty, entry_price))
            pos_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        log.info(f"📥 买入记录: {symbol} ${amount_usd} 结算日:{settle_date} ID:{pos_id}")
        return pos_id

    def record_sell(self, symbol: str, sell_price: float = 0.0) -> bool:
        """记录卖出，关闭最早的同标的仓位"""
        today = datetime.now(ET).date()
        conn = self._get_conn()
        with conn.cursor() as cur:
            # 找最早的 open 仓位
            cur.execute("""
                SELECT id, amount_usd, qty, entry_price FROM t1_positions
                WHERE account = %s AND symbol = %s AND status = 'open'
                ORDER BY buy_date ASC LIMIT 1
            """, (self.account, symbol))
            row = cur.fetchone()
            if not row:
                conn.close()
                log.warning(f"未找到 {symbol} 的开仓记录")
                return False

            pos_id, amount_usd, qty, entry_price = row
            pnl = (sell_price - float(entry_price)) * qty if sell_price and qty else 0

            cur.execute("""
                UPDATE t1_positions
                SET status = 'closed',
                    sell_date = %s,
                    sell_price = %s,
                    pnl_usd = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (today, sell_price, round(pnl, 2), pos_id))
        conn.commit()
        conn.close()
        log.info(f"📤 卖出记录: {symbol} 价格:{sell_price} PnL:${pnl:.2f} ID:{pos_id}")
        return True

    def status_report(self) -> str:
        """生成账户状态报告"""
        positions  = self.get_open_positions()
        available  = self.get_settled_cash()
        unsettled  = self.get_unsettled_sells()
        today      = datetime.now(ET).date()

        lines = [
            "══════════════════════════════════",
            "  IB 现金账户 T+1 资金状态",
            "══════════════════════════════════",
            f"  总资金:     ${ACCOUNT_BALANCE:.2f} USD",
            f"  可用资金:   ${available:.2f} USD",
            f"  占用仓位:   {len(positions)} / {MAX_CONCURRENT} 笔",
            "──────────────────────────────────",
        ]

        if positions:
            lines.append("  当前持仓:")
            for p in positions:
                settle = p["settle_date"]
                settled = "✅已结算" if settle <= today else f"⏳{settle}结算"
                lines.append(f"    {p['symbol']:8} ${p['amount_usd']:.0f}  {settled}")
        else:
            lines.append("  无持仓")

        if unsettled:
            lines.append("──────────────────────────────────")
            lines.append("  今日卖出（未结算）:")
            for p in unsettled:
                lines.append(f"    {p['symbol']:8} ${p['amount_usd']:.0f}  结算:{p['settle_date']}")

        lines.append("══════════════════════════════════")
        return "\n".join(lines)


# ── 命令行测试 ────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    tracker = T1Tracker()
    print(tracker.status_report())

    # 测试买入检查
    ok, reason = tracker.can_buy("ACMR", 400.0)
    print(f"\n买入检查 ACMR $400: {'✅' if ok else '❌'} {reason}")

    ok, reason = tracker.can_buy("CLYM", 400.0)
    print(f"买入检查 CLYM $400: {'✅' if ok else '❌'} {reason}")


# ── BMO RESP 持仓追踪 ─────────────────────────────────────────
class BMOTracker:
    """
    BMO RESP 手动持仓追踪
    买入后手动录入，系统自动计算盈亏、持仓天数、止损止盈
    """
    def __init__(self):
        self._ensure_table()

    def _get_conn(self):
        return get_pg_conn()

    def _ensure_table(self):
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bmo_positions (
                    id           SERIAL PRIMARY KEY,
                    symbol       TEXT NOT NULL,
                    qty          INTEGER NOT NULL,
                    entry_price  NUMERIC(10,4) NOT NULL,
                    entry_date   DATE NOT NULL DEFAULT CURRENT_DATE,
                    stop_price   NUMERIC(10,4),
                    target_price NUMERIC(10,4),
                    commission   NUMERIC(6,2) DEFAULT 10.0,
                    status       TEXT DEFAULT 'open',
                    exit_price   NUMERIC(10,4),
                    exit_date    DATE,
                    notes        TEXT,
                    created_at   TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
        conn.close()

    def add_position(self, symbol: str, qty: int, entry_price: float,
                     stop_price: float = None, target_price: float = None,
                     notes: str = "") -> int:
        """录入 BMO 买入"""
        # 默认止损-3%，止盈+15%（1:5盈亏比）
        if not stop_price:
            stop_price = round(entry_price * 0.97, 2)
        if not target_price:
            target_price = round(entry_price * 1.15, 2)

        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bmo_positions
                  (symbol, qty, entry_price, stop_price, target_price, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (symbol.upper(), qty, entry_price, stop_price, target_price, notes))
            pos_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        return pos_id

    def get_open_positions(self) -> list[dict]:
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *, 
                       CURRENT_DATE - entry_date AS holding_days
                FROM bmo_positions
                WHERE status = 'open'
                ORDER BY entry_date DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def close_position(self, pos_id: int, exit_price: float) -> dict:
        """平仓，计算盈亏"""
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM bmo_positions WHERE id=%s", (pos_id,))
            pos = dict(cur.fetchone())
            gross_pnl = (exit_price - float(pos["entry_price"])) * pos["qty"]
            net_pnl   = gross_pnl - float(pos["commission"]) * 2  # 进+出
            cur.execute("""
                UPDATE bmo_positions
                SET status='closed', exit_price=%s, exit_date=CURRENT_DATE
                WHERE id=%s
            """, (exit_price, pos_id))
        conn.commit()
        conn.close()
        return {"gross_pnl": round(gross_pnl, 2), "net_pnl": round(net_pnl, 2)}

    def status_report(self) -> str:
        positions = self.get_open_positions()
        lines = [
            "══════════════════════════════════",
            "  BMO RESP 持仓状态",
            "══════════════════════════════════",
        ]
        if not positions:
            lines.append("  无持仓")
        for p in positions:
            cost = float(p["entry_price"]) * p["qty"] + float(p["commission"])
            lines.append(
                f"  {p['symbol']:8} {p['qty']}股 @ ${p['entry_price']} "
                f"持仓{p['holding_days']}天 成本${cost:.0f}CAD"
            )
            lines.append(
                f"           止损${p['stop_price']} 止盈${p['target_price']}"
            )
        lines.append("══════════════════════════════════")
        return "\n".join(lines)
