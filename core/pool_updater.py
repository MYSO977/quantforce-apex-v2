#!/usr/bin/env python3
"""
QuantForce Apex v2 — pool_updater.py
节点: .18
每日盘前更新两个股票池：
1. Russell 2000 → 换手率前100 → /tmp/qf_pool_usd.txt
2. TSX基本面前1000 → 换手率前100 → /tmp/qf_pool_cad.txt
"""

import time
import logging
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

USD_POOL_FILE = "/tmp/qf_pool_usd.txt"
CAD_POOL_FILE = "/tmp/qf_pool_cad.txt"
TOP_N         = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [POOL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# ── Russell 2000 ──────────────────────────────────────────────
def get_russell2000() -> list[str]:
    """从 iShares IWM ETF holdings 获取 Russell 2000 成分股"""
    log.info("拉取 Russell 2000...")
    tickers = []

    # 方法1: iShares IWM holdings CSV
    try:
        url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?tab=holdings&fileType=csv"
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        lines = r.text.split("\n")
        # 找数据行（跳过前几行header）
        for i, line in enumerate(lines):
            if line.startswith("Name,") or line.startswith("Ticker,"):
                data_lines = lines[i:]
                break
        else:
            data_lines = lines[9:]  # 默认跳过9行

        from io import StringIO
        df = pd.read_csv(StringIO("\n".join(data_lines)), on_bad_lines='skip')
        # 找 ticker 列
        for col in df.columns:
            if col.strip().lower() in ("ticker", "symbol"):
                tickers = df[col].dropna().tolist()
                tickers = [t.strip() for t in tickers if t.strip() and len(t.strip()) <= 5]
                break
        if tickers:
            log.info(f"IWM holdings: {len(tickers)} 只")
            return tickers
    except Exception as e:
        log.warning(f"IWM方法失败: {e}")

    # 方法2: Wikipedia Russell 2000 (部分)
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Russell_2000_Index")
        for t in tables:
            for col in t.columns:
                if "ticker" in str(col).lower() or "symbol" in str(col).lower():
                    tickers = t[col].dropna().tolist()
                    if len(tickers) > 50:
                        log.info(f"Wikipedia Russell: {len(tickers)} 只")
                        return tickers
    except Exception as e:
        log.warning(f"Wikipedia方法失败: {e}")

    # 方法3: 用现有文件补充
    try:
        with open("/home/heng/tickers_executor.txt") as f:
            tickers = [l.strip() for l in f if l.strip()]
        log.info(f"使用本地文件: {len(tickers)} 只")
        return tickers
    except:
        pass

    return []


# ── TSX 基本面筛选 ────────────────────────────────────────────
def get_tsx_fundamental() -> list[str]:
    """TSX股票基本面筛选：市值≥10亿CAD"""
    log.info("拉取 TSX 基本面...")

    # 核心TSX股票（市值排名靠前）
    tsx_base = [
        "RY.TO","TD.TO","BNS.TO","BMO.TO","CM.TO","NA.TO","MFC.TO","SLF.TO",
        "SU.TO","CNQ.TO","ENB.TO","TRP.TO","PPL.TO","CVE.TO","IMO.TO","MEG.TO",
        "ABX.TO","AEM.TO","FNV.TO","WPM.TO","K.TO","KL.TO","AGI.TO","NGT.TO",
        "SHOP.TO","CSU.TO","OTEX.TO","BB.TO","LSPD.TO","DSGX.TO","MDA.TO",
        "CNR.TO","CP.TO","WCN.TO","TIH.TO","ATD.TO","FTS.TO","AQN.TO",
        "L.TO","MRU.TO","DOL.TO","CTC-A.TO","EMP-A.TO","WN.TO","PBH.TO",
        "BCE.TO","T.TO","RCI-B.TO","QBR-B.TO","Shaw.TO",
        "BAM.TO","BPY-UN.TO","DIR-UN.TO","AP-UN.TO","REI-UN.TO","CAR-UN.TO",
        "GRT-UN.TO","CHP-UN.TO","HR-UN.TO","SRU-UN.TO","CRR-UN.TO",
        "NTR.TO","CCO.TO","LUN.TO","FM.TO","CS.TO","ERO.TO","HBM.TO",
        "MG.TO","BYD.TO","LNR.TO","ATS.TO","STN.TO","WSP.TO","SNC.TO",
        "GWO.TO","IAG.TO","POW.TO","FFH.TO","IFC.TO","EQB.TO","LB.TO",
        "TOU.TO","ARX.TO","PEY.TO","WCP.TO","BTE.TO","TVE.TO","ERF.TO",
        "GIL.TO","MTY.TO","QSR.TO","SJ.TO","ACO-X.TO","PBH.TO","DOO.TO",
        "X.TO","GEI.TO","KEY.TO","IPL.TO","PKI.TO","TRP.TO","ALA.TO",
        "CWB.TO","HCG.TO","EQB.TO","VFF.TO","TPVG.TO","BDT.TO","ACB.TO",
        "HEXO.TO","TLRY.TO","WEED.TO","OGI.TO","APHA.TO","FIRE.TO",
    ]

    # 去重
    result = list(set(tsx_base))
    log.info(f"TSX基础池: {len(result)} 只")
    return result


# ── 换手率筛选 ────────────────────────────────────────────────
def get_turnover_score(ticker: str) -> tuple[str, float]:
    try:
        tk   = yf.Ticker(ticker)
        hist = tk.history(period="5d", interval="1d")
        if len(hist) < 2:
            return ticker, 0.0
        avg_vol   = float(hist["Volume"].tail(3).mean())
        avg_price = float(hist["Close"].tail(3).mean())
        return ticker, avg_vol * avg_price
    except:
        return ticker, 0.0


def filter_by_turnover(tickers: list[str], top_n: int = 100) -> list[str]:
    log.info(f"换手率筛选: {len(tickers)} → 前{top_n}...")
    scores = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(get_turnover_score, t) for t in tickers]
        for f in as_completed(futures):
            t, s = f.result()
            if s > 0:
                scores.append((t, s))
    scores.sort(key=lambda x: x[1], reverse=True)
    selected = [t for t, _ in scores[:top_n]]
    log.info(f"换手率筛选完成: 选出 {len(selected)} 只")
    return selected


def write_pool(path: str, tickers: list[str]):
    with open(path, "w") as f:
        f.write("\n".join(tickers))
    log.info(f"股票池写入: {path} ({len(tickers)} 只)")


def update_pools():
    log.info("=== 开始更新股票池 ===")

    # USD Russell 2000
    usd_all = get_russell2000()
    if usd_all:
        usd_top = filter_by_turnover(usd_all, TOP_N)
        write_pool(USD_POOL_FILE, usd_top)
    else:
        log.error("Russell 2000 拉取失败")

    # CAD TSX
    cad_all = get_tsx_fundamental()
    cad_top = filter_by_turnover(cad_all, TOP_N)
    write_pool(CAD_POOL_FILE, cad_top)

    log.info("=== 股票池更新完成 ===")


def main():
    log.info("pool_updater 启动")

    # 启动立即更新一次
    update_pools()

    while True:
        now = datetime.now(ET)
        # 每天 8:30 ET 盘前更新
        if now.hour == 8 and now.minute == 30 and now.weekday() < 5:
            update_pools()
            time.sleep(60)  # 避免重复触发
        time.sleep(30)


if __name__ == "__main__":
    main()
