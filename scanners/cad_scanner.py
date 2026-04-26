#!/usr/bin/env python3
"""
QuantForce Apex v2 — cad_scanner.py
BMO RESP 加元股票扫描器
节点: .143
股票池: TSX基本面前1000 → 近三日换手率前100 → 技术过滤
"""

import time
import logging
import threading
import random
import psycopg2
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

# ─── CONFIG ──────────────────────────────────────────────────
NODE_NAME     = "compute_143"
MAX_WORKERS   = 5
SCAN_INTERVAL = 300        # 5分钟
COOLDOWN_MIN  = 60
PRICE_MIN     = 2.0        # CAD
PRICE_MAX     = 200.0      # CAD
MIN_AVG_VOL   = 100_000    # 加拿大流动性要求低一些
RVOL_MIN      = 1.5
MIN_MKTCAP    = 1e9        # 最低10亿CAD市值（基本面前1000门槛）

PG_HOST = "192.168.0.18"
PG_PORT = 5432
PG_DB   = "quantforce"
PG_USER = "postgres"
PG_PASS = "newpassword123"
# ─────────────────────────────────────────────────────────────

ET = ZoneInfo("America/New_York")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CAD_SCANNER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

_cooldown: dict[str, datetime] = {}
_cooldown_lock = threading.Lock()


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=5
    )


def get_tsx_tickers() -> list[str]:
    """
    获取TSX股票池：
    1. TSX 60（蓝筹）
    2. TSX Composite 部分成分
    3. 手动补充的高流动性TSX股票
    全部加 .TO 后缀
    """
    tickers = set()

    # TSX 60 from Wikipedia
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/S%26P/TSX_60")
        for t in tables:
            for col in t.columns:
                if "ticker" in col.lower() or "symbol" in col.lower():
                    tickers.update(t[col].dropna().tolist())
                    break
        log.info(f"TSX 60 加载: {len(tickers)} 只")
    except Exception as e:
        log.warning(f"TSX 60 加载失败: {e}")

    # 补充常见高流动性TSX股票
    supplement = [
        "SU.TO", "CNQ.TO", "RY.TO", "TD.TO", "BNS.TO", "BMO.TO", "CM.TO", "NA.TO",
        "MFC.TO", "SLF.TO", "GWO.TO", "IAG.TO", "POW.TO", "FFH.TO",
        "ENB.TO", "TRP.TO", "PPL.TO", "KEY.TO", "IPL.TO",
        "ABX.TO", "AEM.TO", "FNV.TO", "WPM.TO", "K.TO", "KL.TO", "AGI.TO",
        "SHOP.TO", "CSU.TO", "OTEX.TO", "BB.TO", "DSGX.TO",
        "CNR.TO", "CP.TO", "WCN.TO", "TIH.TO", "ATD.TO",
        "L.TO", "MRU.TO", "EMP-A.TO", "DOL.TO", "CTC-A.TO",
        "BCE.TO", "T.TO", "RCI-B.TO", "QBR-B.TO",
        "BAM.TO", "BPY-UN.TO", "DIR-UN.TO", "AP-UN.TO", "REI-UN.TO",
        "NTR.TO", "AG.TO", "CCO.TO", "LUN.TO", "FM.TO", "CS.TO",
        "CAR-UN.TO", "GRT-UN.TO", "CHP-UN.TO", "SMU-UN.TO",
        "TOU.TO", "ARX.TO", "PEY.TO", "WCP.TO", "BTE.TO", "MEG.TO",
        "MG.TO", "ABC.TO", "BYD.TO", "MDA.TO", "HPS-A.TO",
        "STN.TO", "WSP.TO", "ATS.TO", "TXT.TO",
        "EQB.TO", "CWB.TO", "LB.TO", "HCG.TO",
        "ACO-X.TO", "GIL.TO", "PBH.TO", "MTY.TO", "QSR.TO",
        "SJ.TO", "IFC.TO", "X.TO", "ERF.TO", "GEI.TO",
    ]
    tickers.update(supplement)

    # 确保全部有 .TO 后缀
    result = []
    for t in tickers:
        t = str(t).strip()
        if not t.endswith(".TO") and not t.endswith(".V"):
            t = t + ".TO"
        result.append(t)

    result = list(set(result))
    log.info(f"TSX股票池总计: {len(result)} 只")
    return result


def filter_by_turnover(tickers: list[str], top_n: int = 100) -> list[str]:
    """
    近三日换手率前100筛选
    换手率 = 近三日日均成交量 / 流通股数
    用日均成交额近似（成交量×价格）
    """
    log.info(f"换手率筛选: {len(tickers)} → 前{top_n}...")
    scores = []

    def get_turnover(ticker):
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(period="5d", interval="1d")
            if len(hist) < 2:
                return None
            avg_vol = hist["Volume"].tail(3).mean()
            avg_price = hist["Close"].tail(3).mean()
            turnover = avg_vol * avg_price  # 近似换手额
            return (ticker, turnover)
        except:
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(get_turnover, t) for t in tickers]
        for f in as_completed(futures):
            r = f.result()
            if r:
                scores.append(r)

    scores.sort(key=lambda x: x[1], reverse=True)
    selected = [t for t, _ in scores[:top_n]]
    log.info(f"换手率筛选完成: 选出 {len(selected)} 只")
    return selected


def is_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


def analyze_ticker(ticker: str) -> dict | None:
    try:
        time.sleep(random.uniform(0.1, 0.5))
        tk = yf.Ticker(ticker)

        df1 = tk.history(period="1d", interval="1m")
        if df1.empty or len(df1) < 10:
            return None

        df5 = tk.history(period="5d", interval="5m")
        if df5.empty or len(df5) < 30:
            return None

        current_price = float(df1["Close"].iloc[-1])
        open_price    = float(df1["Open"].iloc[0])

        if not (PRICE_MIN <= current_price <= PRICE_MAX):
            return None

        # VWAP
        df1["tp"]  = (df1["High"] + df1["Low"] + df1["Close"]) / 3
        df1["tpv"] = df1["tp"] * df1["Volume"]
        vwap = float(df1["tpv"].cumsum().iloc[-1] / df1["Volume"].cumsum().iloc[-1])

        # RVOL
        now_et  = datetime.now(ET)
        elapsed = max((now_et.hour * 60 + now_et.minute) - (9 * 60 + 30), 1)
        cum_vol = float(df1["Volume"].sum())
        df5d    = tk.history(period="5d", interval="1d")
        if len(df5d) >= 2:
            avg_daily = float(df5d["Volume"].iloc[:-1].mean())
            avg_now   = avg_daily * (elapsed / 390)
        else:
            avg_now = cum_vol
        rvol = cum_vol / avg_now if avg_now > 0 else 0

        # EMA9
        closes5 = df5["Close"]
        ema9 = closes5.ewm(span=9, adjust=False).mean()
        cond_ema9 = float(ema9.iloc[-1]) > float(ema9.iloc[-2])

        # MACD
        ema12 = closes5.ewm(span=12, adjust=False).mean()
        ema26 = closes5.ewm(span=26, adjust=False).mean()
        macd_line = float((ema12 - ema26).iloc[-1])

        # 五个条件
        cond_rvol  = rvol >= RVOL_MIN
        cond_vwap  = current_price > vwap
        cond_macd  = macd_line > 0
        cond_open  = current_price > open_price

        if not all([cond_rvol, cond_vwap, cond_ema9, cond_macd, cond_open]):
            return None

        return {
            "ticker":  ticker,
            "price":   round(current_price, 2),
            "open":    round(open_price, 2),
            "rvol":    round(rvol, 2),
            "vwap":    round(vwap, 2),
            "macd":    round(macd_line, 4),
            "ema9":    round(float(ema9.iloc[-1]), 4),
            "score":   7.5,
            "source":  f"cad_scanner_{NODE_NAME}",
            "ts":      datetime.now(ET).isoformat()
        }
    except Exception as e:
        log.debug(f"{ticker} 分析失败: {e}")
        return None


def check_cooldown(ticker: str) -> bool:
    with _cooldown_lock:
        last = _cooldown.get(ticker)
        if last and (datetime.now(ET) - last) < timedelta(minutes=COOLDOWN_MIN):
            return False
        _cooldown[ticker] = datetime.now(ET)
        return True


def write_signal(sig: dict):
    """写入 signals_raw，标记为 bmo_resp 账户"""
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO signals_raw
                  (signal_id, symbol, signal_type, direction,
                   confidence, score, source, pipeline, features)
                VALUES (%s, %s, 'tech', 'buy', %s, %s, %s, 'cad_apex', %s)
                ON CONFLICT (signal_id) DO NOTHING
            """, (
                str(uuid.uuid4()),
                sig["ticker"],
                8.0,           # confidence
                sig["score"],
                sig["source"],
                psycopg2.extras.Json({
                    "price":    sig["price"],
                    "open":     sig["open"],
                    "rvol":     sig["rvol"],
                    "vwap":     sig["vwap"],
                    "macd":     sig["macd"],
                    "ema9":     sig["ema9"],
                    "score":    sig["score"],
                    "ticker":   sig["ticker"],
                    "currency": "CAD",
                    "account":  "bmo_resp",
                    "source":   sig["source"],
                })
            ))
        conn.commit()
        conn.close()
        log.info(f"✅ 信号写入PG: {sig['ticker']} RVOL={sig['rvol']} 价格={sig['price']} CAD")
    except Exception as e:
        log.error(f"信号写入失败 {sig['ticker']}: {e}")


def run_scan(tickers: list[str]):
    if not is_market_open():
        log.info("非交易时间，跳过")
        return

    log.info(f"开始扫描 {len(tickers)} 只TSX股票...")
    t0 = time.time()
    signals = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                ticker = result["ticker"]
                if check_cooldown(ticker):
                    signals.append(result)
                    write_signal(result)

    elapsed = round(time.time() - t0, 1)
    log.info(f"扫描完成，耗时 {elapsed}s，触发信号 {len(signals)} 个")


def main():
    log.info(f"=== cad_scanner 启动 [{NODE_NAME}] ===")

    # 启动时加载完整股票池
    all_tickers = get_tsx_tickers()

    # 换手率筛选（每日盘前更新一次）
    active_tickers = filter_by_turnover(all_tickers, top_n=100)

    last_filter_date = datetime.now(ET).date()

    while True:
        try:
            # 每天盘前9点重新筛选换手率
            today = datetime.now(ET).date()
            now_hour = datetime.now(ET).hour
            if today != last_filter_date and now_hour < 9:
                log.info("每日换手率重新筛选...")
                active_tickers = filter_by_turnover(all_tickers, top_n=100)
                last_filter_date = today

            run_scan(active_tickers)

        except Exception as e:
            log.error(f"扫描异常: {e}")

        log.info(f"等待 {SCAN_INTERVAL}s 后下一轮...")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
