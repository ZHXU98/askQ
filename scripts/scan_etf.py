#!/usr/bin/env python3
"""
scan_etf.py — ETF 行情扫描与分析脚本
======================================
功能：
1. 【行情排行】全市场 ETF 今日涨跌幅排行（涨幅榜/跌幅榜）
2. 【动量筛选】近5/20日涨幅靠前的 ETF（趋势追踪）
3. 【超跌筛选】从高点回撤 > 30% 的 ETF（低吸候选）
4. 【核心 ETF 监控】预设核心宽基/行业 ETF 的实时行情汇总
5. 【用户 ETF 持仓】读取 my_etf_portfolio.json，输出含成本的持仓报告
6. 【估值参考】主要宽基指数 PE/PB 历史分位（辅助择时）
7. 【资金流向】ETF 一级市场份额变化 + 全市场资金流向分类排行

依赖：pip install akshare pandas

用法：
  python3 scripts/scan_etf.py                    # 全量扫描
  python3 scripts/scan_etf.py --mode rank        # 今日涨跌幅排行
  python3 scripts/scan_etf.py --mode momentum    # 动量筛选（趋势ETF）
  python3 scripts/scan_etf.py --mode oversold    # 超跌筛选（低吸候选）
  python3 scripts/scan_etf.py --mode core        # 核心ETF监控
  python3 scripts/scan_etf.py --mode portfolio   # 我的ETF持仓
  python3 scripts/scan_etf.py --mode valuation   # 估值参考
  python3 scripts/scan_etf.py --mode flow        # 资金流向（份额变化+全市场分类）
  python3 scripts/scan_etf.py --json             # JSON格式输出
"""

import json
import sys
import argparse
import time
import functools
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Optional, List, Dict, Any

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import akshare as ak
    import pandas as pd
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False

# ── 路径
ROOT_DIR = Path(__file__).parent.parent
ETF_PORTFOLIO_FILE = ROOT_DIR / "my_etf_portfolio.json"

# ── 核心 ETF 监控列表（宽基 + 主要行业）
CORE_ETF_LIST = [
    # 宽基
    {"code": "510300", "name": "沪深300ETF", "category": "宽基", "market": "sh"},
    {"code": "510500", "name": "中证500ETF", "category": "宽基", "market": "sh"},
    {"code": "159915", "name": "创业板ETF",  "category": "宽基", "market": "sz"},
    {"code": "510050", "name": "上证50ETF",  "category": "宽基", "market": "sh"},
    {"code": "159338", "name": "中证A500ETF","category": "宽基", "market": "sz"},
    {"code": "588000", "name": "科创50ETF",  "category": "宽基", "market": "sh"},
    {"code": "510880", "name": "红利ETF",    "category": "宽基", "market": "sh"},
    # 行业
    {"code": "512480", "name": "半导体ETF",  "category": "科技", "market": "sh"},
    {"code": "515070", "name": "人工智能ETF","category": "科技", "market": "sh"},
    {"code": "516160", "name": "新能源ETF",  "category": "新能源","market": "sh"},
    {"code": "512010", "name": "医药ETF",    "category": "医药", "market": "sh"},
    {"code": "512660", "name": "军工ETF",    "category": "军工", "market": "sh"},
    {"code": "512800", "name": "银行ETF",    "category": "金融", "market": "sh"},
    {"code": "512880", "name": "券商ETF",    "category": "金融", "market": "sh"},
    {"code": "159928", "name": "消费ETF",    "category": "消费", "market": "sz"},
    {"code": "513100", "name": "纳指ETF",    "category": "跨境", "market": "sh"},
    {"code": "518880", "name": "黄金ETF",    "category": "商品", "market": "sh"},
]

# ── 指数估值配置（支持 stock_index_pe_lg 的指数名）
INDEX_VALUATION_CONFIG = [
    {"name": "沪深300",  "etf_code": "510300", "years": 10},
    {"name": "上证50",   "etf_code": "510050", "years": 10},
    {"name": "中证500",  "etf_code": "510500", "years": 10},
    {"name": "中证1000", "etf_code": "159845", "years": 5},
]


def _safe_float(val) -> Optional[float]:
    try:
        v = float(val)
        return v if (v == v) else None
    except (TypeError, ValueError):
        return None


def _get_last_trading_date(offset: int = 0) -> str:
    """获取最近第 offset 个交易日（offset=0为最近）"""
    d = datetime.now()
    count = 0
    for i in range(30):
        candidate = d - timedelta(days=i)
        if candidate.weekday() < 5:
            if count == offset:
                return candidate.strftime("%Y%m%d")
            count += 1
    return d.strftime("%Y%m%d")


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v:+.2f}%"


def _fmt_amount(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    if abs(v) >= 1e8:
        return f"{v/1e8:.2f}亿"
    return f"{v/1e4:.0f}万"


# ──────────────────────────────────────────────
# 网络请求重试装饰器
# ──────────────────────────────────────────────

def retry_on_failure(max_retries: int = 3, delay: float = 2.0, backoff: float = 2.0):
    """
    网络请求重试装饰器（指数退避）
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 退避倍数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_delay = delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"⚠️  第{attempt + 1}次尝试失败: {str(e)[:50]}")
                        print(f"   等待 {retry_delay:.1f} 秒后重试...")
                        time.sleep(retry_delay)
                        retry_delay *= backoff
            
            # 所有重试都失败
            raise last_exception
        return wrapper
    return decorator


# ──────────────────────────────────────────────
# 1. 获取全市场 ETF 实时行情
# ──────────────────────────────────────────────

def _is_trading_time() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return time(9, 30) <= t <= time(15, 0)


def _get_realtime_price_sina(sina_symbol: str) -> Optional[Dict]:
    """
    用新浪分钟线接口获取单只 ETF 的盘中实时价格。
    sina_symbol: 如 'sh510300', 'sz159915'
    返回 {"price": float, "change_pct": float, "amount": None, "data_type": "realtime(HH:MM)"}
    """
    try:
        df_m = ak.stock_zh_a_minute(symbol=sina_symbol, period="5", adjust="")
        if df_m is None or df_m.empty:
            return None
        df_m = df_m.sort_values("day")
        latest = df_m.iloc[-1]
        price = _safe_float(latest.get("close"))
        trade_time = str(latest.get("day", ""))[:16]
        # 昨收：用历史日线最后一条
        return {"price": price, "data_type": f"realtime({trade_time})", "change_pct": None}
    except Exception:
        return None


def get_all_etf_spot() -> pd.DataFrame:
    """
    获取核心 ETF 行情：
    - 交易时段（09:30-15:00）：用新浪分钟线获取最新实时价，与昨收对比算涨跌幅
    - 非交易时段：用新浪日线获取最新收盘价
    东财接口在本机网络环境下断连，统一走新浪接口。
    """
    if not HAS_AKSHARE:
        return pd.DataFrame()

    trading = _is_trading_time()
    rows = []

    for etf in CORE_ETF_LIST:
        code = etf["code"]
        market = etf["market"]
        sina_symbol = f"{market}{code}"

        try:
            # 拉取新浪日线（提供昨收 + 历史数据，盘后也用它）
            df_h = ak.fund_etf_hist_sina(symbol=sina_symbol)
            if df_h is None or df_h.empty:
                continue
            df_h = df_h.sort_values("date")
            prev_row = df_h.iloc[-1]          # 最近一个收盘日
            prev_close = _safe_float(prev_row.get("close"))
            prev_amount = _safe_float(prev_row.get("amount"))
            prev_date   = str(prev_row.get("date", ""))[:10]

            if trading:
                # 交易时段：用分钟线拿实时价
                rt = _get_realtime_price_sina(sina_symbol)
                if rt and rt["price"]:
                    price = rt["price"]
                    change_pct = (
                        (price - prev_close) / prev_close * 100
                        if prev_close and prev_close > 0 else None
                    )
                    rows.append({
                        "code":       code,
                        "name":       etf["name"],
                        "price":      price,
                        "change_pct": change_pct,
                        "amount":     None,          # 分钟线无当日累计成交额
                        "data_type":  rt["data_type"],
                        "category":   etf["category"],
                    })
                    continue
                # 分钟线失败，降级用昨收
            # 非交易时段或分钟线失败：用最新日线收盘价
            prev_close2 = _safe_float(df_h.iloc[-2].get("close")) if len(df_h) >= 2 else None
            change_pct = (
                (prev_close - prev_close2) / prev_close2 * 100
                if prev_close and prev_close2 and prev_close2 > 0 else None
            )
            rows.append({
                "code":       code,
                "name":       etf["name"],
                "price":      prev_close,
                "change_pct": change_pct,
                "amount":     prev_amount,
                "data_type":  f"close({prev_date})",
                "category":   etf["category"],
            })
        except Exception:
            continue

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame()


# ──────────────────────────────────────────────
# 2. 今日涨跌幅排行
# ──────────────────────────────────────────────

def get_etf_rank(df: pd.DataFrame, top_n: int = 15) -> Dict[str, List[Dict]]:
    """
    返回涨幅榜和跌幅榜
    """
    if df.empty or "change_pct" not in df.columns:
        return {"top_gain": [], "top_loss": []}

    df_sorted = df.sort_values("change_pct", ascending=False)
    # 过滤连续停牌/异常值（涨跌幅超20%视为异常，科创板除外）
    df_valid = df_sorted[df_sorted["change_pct"].abs() <= 25]

    def row_to_dict(row) -> Dict:
        return {
            "code":       str(row.get("code", "")),
            "name":       str(row.get("name", "")),
            "price":      _safe_float(row.get("price")),
            "change_pct": _safe_float(row.get("change_pct")),
            "amount":     _safe_float(row.get("amount")),
        }

    top_gain = [row_to_dict(r) for _, r in df_valid.head(top_n).iterrows()]
    top_loss = [row_to_dict(r) for _, r in df_valid.tail(top_n).iloc[::-1].iterrows()]
    return {"top_gain": top_gain, "top_loss": top_loss}


# ──────────────────────────────────────────────
# 3. 动量筛选（近N日趋势追踪）
# ──────────────────────────────────────────────

def get_etf_momentum(top_n: int = 10) -> List[Dict]:
    """
    获取近20日涨幅最大的 ETF（动量策略）
    限定在核心 ETF 列表中，避免小众 ETF 干扰
    """
    if not HAS_AKSHARE:
        return []

    results = []
    end_date = _get_last_trading_date()
    start_date_20 = _get_last_trading_date(20)

    for etf in CORE_ETF_LIST:
        code = etf["code"]
        market = etf["market"]
        try:
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date_20,
                end_date=end_date,
                adjust="qfq",
            )
            if df is None or len(df) < 5:
                continue
            df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
            close_first = _safe_float(df.iloc[0]["收盘"])
            close_last  = _safe_float(df.iloc[-1]["收盘"])
            if close_first and close_last and close_first > 0:
                gain_20d = (close_last - close_first) / close_first * 100
                results.append({
                    "code":     code,
                    "name":     etf["name"],
                    "category": etf["category"],
                    "price":    close_last,
                    "gain_20d": gain_20d,
                })
        except Exception:
            continue

    results.sort(key=lambda x: -x.get("gain_20d", 0))
    return results[:top_n]


# ──────────────────────────────────────────────
# 4. 超跌筛选（从高点回撤）
# ──────────────────────────────────────────────

def get_etf_oversold(drawdown_threshold: float = 0.25, top_n: int = 10) -> List[Dict]:
    """
    筛选近1年内从高点回撤超过指定阈值的核心 ETF（超跌候选）
    默认阈值 25%
    """
    if not HAS_AKSHARE:
        return []

    results = []
    end_date   = _get_last_trading_date()
    start_date = _get_last_trading_date(250)  # 约1年

    for etf in CORE_ETF_LIST:
        code = etf["code"]
        try:
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is None or len(df) < 20:
                continue
            df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce").fillna(0)
            peak = df["收盘"].max()
            current = _safe_float(df.iloc[-1]["收盘"])
            if peak > 0 and current:
                drawdown = (peak - current) / peak
                if drawdown >= drawdown_threshold:
                    results.append({
                        "code":      code,
                        "name":      etf["name"],
                        "category":  etf["category"],
                        "price":     current,
                        "peak":      peak,
                        "drawdown":  drawdown,
                    })
        except Exception:
            continue

    results.sort(key=lambda x: -x.get("drawdown", 0))
    return results[:top_n]


# ──────────────────────────────────────────────
# 5. 核心 ETF 监控行情
# ──────────────────────────────────────────────

def get_core_etf_quotes(spot_df: pd.DataFrame) -> List[Dict]:
    """
    从全市场行情中提取核心 ETF 的实时数据
    """
    if spot_df.empty:
        return []

    results = []
    for etf in CORE_ETF_LIST:
        code = etf["code"]
        row = spot_df[spot_df["code"] == code] if not spot_df.empty else pd.DataFrame()

        # 历史模式：spot_df 本身就是从 CORE_ETF_LIST 逐只拼的，可能直接有数据
        if row.empty and not spot_df.empty and "category" in spot_df.columns:
            row = spot_df[spot_df["code"] == code]

        if row.empty:
            results.append({
                "code":       code,
                "name":       etf["name"],
                "category":   etf["category"],
                "price":      None,
                "change_pct": None,
                "amount":     None,
            })
            continue
        r = row.iloc[0]
        results.append({
            "code":       code,
            "name":       etf.get("name") or str(r.get("name", code)),
            "category":   etf["category"],
            "price":      _safe_float(r.get("price")),
            "change_pct": _safe_float(r.get("change_pct")),
            "amount":     _safe_float(r.get("amount")),
        })
    return results


# ──────────────────────────────────────────────
# 6. 用户 ETF 持仓
# ──────────────────────────────────────────────

def load_etf_portfolio() -> List[Dict]:
    """读取 my_etf_portfolio.json"""
    if not ETF_PORTFOLIO_FILE.exists():
        return []
    try:
        with open(ETF_PORTFOLIO_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("etfs", [])
    except Exception:
        return []


def _get_etf_market_prefix(code: str) -> str:
    """根据代码推断交易所前缀：sh 或 sz"""
    # 深交所：15xxxx, 16xxxx, 30xxxx
    if code.startswith(("15", "16", "30")):
        return "sz"
    return "sh"


def get_etf_portfolio_report(spot_df: pd.DataFrame) -> List[Dict]:
    """
    结合行情数据，输出含浮盈的 ETF 持仓报告。
    价格优先从 spot_df 取（已含实时数据），
    若 spot_df 中不存在则用新浪分钟线/日线补取。
    """
    holdings = load_etf_portfolio()
    if not holdings:
        return []

    results = []
    for h in holdings:
        code = str(h.get("code", ""))
        name = h.get("name", code)
        cost = _safe_float(h.get("cost_price"))
        shares = _safe_float(h.get("shares"))
        mkt = _get_etf_market_prefix(code)
        sina_symbol = f"{mkt}{code}"

        # 1. 先从 spot_df 取（核心列表命中）
        price = None
        change_pct = None
        if not spot_df.empty and "code" in spot_df.columns:
            row = spot_df[spot_df["code"] == code]
            if not row.empty:
                price      = _safe_float(row.iloc[0].get("price"))
                change_pct = _safe_float(row.iloc[0].get("change_pct"))

        # 2. spot_df 未命中（持仓 ETF 不在核心列表）→ 直接查新浪接口
        if price is None and HAS_AKSHARE:
            try:
                # 先取日线，拿到昨收（作为涨跌幅基准）
                df_h = ak.fund_etf_hist_sina(symbol=sina_symbol)
                prev_close = None
                if df_h is not None and not df_h.empty:
                    df_h = df_h.sort_values("date")
                    prev_close = _safe_float(df_h.iloc[-1].get("close"))

                if _is_trading_time():
                    # 交易时段：分钟线实时价
                    df_m = ak.stock_zh_a_minute(symbol=sina_symbol, period="5", adjust="")
                    if df_m is not None and not df_m.empty:
                        df_m = df_m.sort_values("day")
                        price = _safe_float(df_m.iloc[-1].get("close"))
                        # 与昨收对比算涨跌幅
                        if price and prev_close and prev_close > 0:
                            change_pct = (price - prev_close) / prev_close * 100

                # 非交易时段或分钟线未取到：用日线收盘价
                if price is None and prev_close is not None:
                    price = prev_close
                    if df_h is not None and len(df_h) >= 2:
                        pc2 = _safe_float(df_h.iloc[-2].get("close"))
                        if pc2 and pc2 > 0:
                            change_pct = (price - pc2) / pc2 * 100
            except Exception:
                pass

        # 计算浮盈
        profit_pct = None
        profit_amt = None
        if price and cost and cost > 0:
            profit_pct = (price - cost) / cost * 100
            if shares:
                profit_amt = (price - cost) * shares

        results.append({
            "code":       code,
            "name":       name,
            "cost":       cost,
            "shares":     shares,
            "price":      price,
            "change_pct": change_pct,
            "profit_pct": profit_pct,
            "profit_amt": profit_amt,
            "note":       h.get("note", ""),
        })
    return results


# ──────────────────────────────────────────────
# 7. 估值参考（宽基指数 PE）
# ──────────────────────────────────────────────

def _calc_percentile(series, current_val: float, years: int = 10) -> Optional[float]:
    """计算 current_val 在最近 years 年历史数据中的百分位（0-100）"""
    try:
        n = years * 250  # 约 250 个交易日/年
        hist = series.dropna().astype(float)
        if len(hist) > n:
            hist = hist[-n:]
        if len(hist) < 50:
            return None
        pct = float((hist < current_val).mean() * 100)
        return round(pct, 1)
    except Exception:
        return None


def _pct_signal(pct: Optional[float]) -> str:
    """根据历史分位返回信号emoji"""
    if pct is None:
        return "⚪"
    if pct <= 20:
        return "🟢"
    if pct >= 80:
        return "🔴"
    return "🟡"


def get_portfolio_valuation_report() -> List[Dict]:
    """
    计算用户持仓 ETF 的净值历史分位（替代 PE/PB 分位）。
    用 ak.fund_etf_fund_info_em 获取每只 ETF 的完整净值历史。
    """
    if not HAS_AKSHARE:
        return []

    holdings = load_etf_portfolio()
    if not holdings:
        return []

    results = []
    for h in holdings:
        code  = str(h.get("code", ""))
        name  = h.get("name", code)
        cost  = _safe_float(h.get("cost_price"))
        try:
            df = ak.fund_etf_fund_info_em(fund=code)
            if df is None or df.empty:
                raise ValueError("无净值数据")

            df["单位净值"] = df["单位净值"].astype(float)
            df = df.sort_values("净值日期")

            nav_series = df["单位净值"]
            current_nav = float(nav_series.iloc[-1])
            latest_date = str(df["净值日期"].iloc[-1])[:10]

            # 全历史分位
            pct_all = _calc_percentile(nav_series, current_nav, years=100)
            # 近3年分位（约750个交易日）
            nav_3y = nav_series[-750:] if len(nav_series) > 750 else nav_series
            pct_3y = _calc_percentile(nav_3y, current_nav, years=3)

            # 从历史最高点回撤
            peak = float(nav_series.max())
            drawdown = (current_nav - peak) / peak * 100

            # 成本比较
            cost_pct = (current_nav - cost) / cost * 100 if cost and cost > 0 else None

            # 信号判断（以近3年分位为主）
            sig = _pct_signal(pct_3y)
            if pct_3y is not None:
                if pct_3y <= 25:
                    advice = "历史低位，可定投/加仓"
                elif pct_3y >= 75:
                    advice = "历史高位，控制仓位/止盈观望"
                elif pct_3y <= 40:
                    advice = "偏低位置，可小额定投"
                elif pct_3y >= 60:
                    advice = "偏高位置，注意风险"
                else:
                    advice = "正常持有/小额定投"
            else:
                advice = "历史数据不足"

            results.append({
                "code":        code,
                "name":        name,
                "current_nav": round(current_nav, 4),
                "latest_date": latest_date,
                "pct_all":     pct_all,
                "pct_3y":      pct_3y,
                "drawdown":    round(drawdown, 1),
                "peak":        round(peak, 4),
                "lowest":      round(float(nav_series.min()), 4),
                "data_count":  len(nav_series),
                "cost":        cost,
                "cost_pct":    round(cost_pct, 2) if cost_pct is not None else None,
                "signal":      sig,
                "advice":      advice,
            })
        except Exception as e:
            results.append({
                "code":    code,
                "name":    name,
                "current_nav": None,
                "latest_date": "",
                "pct_all":  None,
                "pct_3y":  None,
                "drawdown": None,
                "peak":    None,
                "lowest":  None,
                "data_count": 0,
                "cost":    cost,
                "cost_pct": None,
                "signal":  "⚪",
                "advice":  f"数据获取失败: {str(e)[:30]}",
            })
    return results


def get_valuation_reference() -> List[Dict]:
    """
    获取主要宽基指数的当前 PE/PB 及历史分位。
    使用 ak.stock_index_pe_lg / stock_index_pb_lg（已验证可用）。
    """
    if not HAS_AKSHARE:
        return []

    results = []
    for cfg in INDEX_VALUATION_CONFIG:
        name     = cfg["name"]
        etf_code = cfg["etf_code"]
        years    = cfg["years"]
        try:
            df_pe = ak.stock_index_pe_lg(symbol=name)
            df_pb = ak.stock_index_pb_lg(symbol=name)

            # PE：用「滚动市盈率」列
            pe_col = "滚动市盈率"
            pb_col = "市净率"

            pe_series = df_pe[pe_col].astype(float)
            pb_series = df_pb[pb_col].astype(float)

            current_pe = float(pe_series.iloc[-1])
            current_pb = float(pb_series.iloc[-1])
            latest_date = str(df_pe["日期"].iloc[-1])[:10]

            pe_pct = _calc_percentile(pe_series, current_pe, years)
            pb_pct = _calc_percentile(pb_series, current_pb, years)

            # 综合信号：以 PE 分位为主
            sig = _pct_signal(pe_pct)
            if pe_pct is not None:
                if pe_pct <= 20:
                    advice = "低估区间，可加仓/定投"
                elif pe_pct >= 80:
                    advice = "高估区间，注意减仓或止盈"
                elif pe_pct <= 40:
                    advice = "偏低，可定投"
                elif pe_pct >= 60:
                    advice = "偏高，控制仓位"
                else:
                    advice = "正常估值区间"
            else:
                advice = "历史数据不足"

            results.append({
                "name":        name,
                "etf_code":    etf_code,
                "pe":          round(current_pe, 2),
                "pb":          round(current_pb, 2),
                "pe_pct":      pe_pct,
                "pb_pct":      pb_pct,
                "pe_signal":   sig,
                "advice":      advice,
                "date":        latest_date,
                "years":       years,
            })
        except Exception as e:
            results.append({
                "name":     name,
                "etf_code": etf_code,
                "pe":       None,
                "pb":       None,
                "pe_pct":   None,
                "pb_pct":   None,
                "pe_signal": "⚪",
                "advice":   f"数据获取失败: {str(e)[:40]}",
                "date":     "",
                "years":    years,
            })
    return results


# ──────────────────────────────────────────────
# 8. 资金流向（一级市场份额变化 + 全市场分类流向）
# ──────────────────────────────────────────────

# ETF 分类关键字映射（用于全市场 ETF 自动分类）
# 注意：关键词按优先级排序，优先匹配更具体的行业/主题
ETF_CATEGORY_KEYWORDS = {
    # 宽基指数（优先级最高）
    "宽基": ["沪深300", "中证500", "中证1000", "上证50", "创业板", "科创", "A500", "A50",
             "中证800", "中证200", "上证180", "深证100", "全指"],
    
    # 行业主题（按活跃度排序）
    "科技": ["半导体", "芯片", "人工智能", "AI", "信息技术", "软件", "云计算", "数字",
             "通信", "5G", "互联网", "大数据", "机器人", "工业40", "工业4.0"],
    
    "新能源": ["新能源", "光伏", "风电", "储能", "电动车", "锂电", "绿电", "碳中和",
               "清洁能源", "新能源车"],
    
    "医药": ["医药", "医疗", "生物", "创新药", "医械", "CXO", "医美", "疫苗"],
    
    "消费": ["消费", "白酒", "食品", "家电", "零售", "品牌", "必选", "可选"],
    
    "金融": ["银行", "券商", "保险", "金融", "地产", "房地产", "证券"],
    
    "资源": ["黄金", "原油", "能源", "煤炭", "有色", "钢铁", "化工", "采矿",
             "资源", "石油", "天然气"],
    
    "红利": ["红利", "高股息", "股息", "分红", "现金流"],
    
    "国企改革": ["国企改革", "国企", "一带一路"],
    
    "环保": ["环保", "环境治理", "环境", "低碳"],
    
    # 跨境投资
    "港股": ["港股", "恒生", "恒科", "恒指", "H股", "南向"],
    
    "海外": ["纳斯达克", "标普", "纳指", "美股", "日经", "德国", "欧洲",
             "印度", "越南", "东南亚", "QDII", "境外"],
    
    # 其他资产类别
    "债券": ["债券", "国债", "企债", "可转债", "利率"],
    
    "商品": ["黄金", "白银", "铜", "铝", "商品"],
    
    # LOF常见主题（需要具体分类的放前面）
    "社会责任": ["社会责任", "ESG", "可持续发展"],
    
    "主题投资": ["创新", "成长", "领先", "先锋", "主题", "未来"],
}

def _classify_etf(name: str) -> str:
    """根据ETF名称自动分类"""
    for cat, keywords in ETF_CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in name:
                return cat
    return "其他"


def get_portfolio_share_flow(days: int = 10) -> List[Dict]:
    """
    获取持仓ETF近N日一级市场份额变化（申购/赎回信号）。
    通过 fund_etf_fund_info_em 获取历史净值+份额数据，
    计算最近 days 天的份额净变化趋势。
    """
    if not HAS_AKSHARE:
        return []

    holdings = load_etf_portfolio()
    if not holdings:
        return []

    # 一次性获取全市场ETF数据（避免在循环中重复获取）
    spot_df_all = None
    try:
        spot_df_all = ak.fund_etf_spot_em()
    except Exception:
        pass  # 如果获取失败，后续份额数据会是 N/A

    results = []
    for h in holdings:
        code = str(h.get("code", ""))
        name = h.get("name", code)
        cost = _safe_float(h.get("cost_price"))
        shares_held = _safe_float(h.get("shares"))

        try:
            df = ak.fund_etf_fund_info_em(fund=code)
            if df is None or df.empty:
                raise ValueError("无数据")

            # 确保列存在
            df = df.copy()
            df["净值日期"] = pd.to_datetime(df["净值日期"], errors="coerce")
            df = df.dropna(subset=["净值日期"]).sort_values("净值日期")

            # 份额列：可能叫"累计份额净值"或"申购"等，实际上 fund_etf_fund_info_em 返回的是净值历史
            # 用"单位净值"来代理：若总市值字段不可用，改用实时行情的最新份额做截面对比
            df["单位净值"] = pd.to_numeric(df["单位净值"], errors="coerce")

            # 取最近 days+2 条数据
            recent = df.tail(days + 2)
            nav_list = recent["单位净值"].dropna().tolist()
            date_list = [str(d)[:10] for d in recent["净值日期"].tolist()]

            # 用 fund_etf_spot_em 补充今日最新份额（截面）
            spot_shares = None
            spot_mkt_val = None
            if spot_df_all is not None:
                row = spot_df_all[spot_df_all["代码"] == code]
                if not row.empty:
                    spot_shares = _safe_float(row.iloc[0].get("最新份额"))
                    spot_mkt_val = _safe_float(row.iloc[0].get("流通市值"))

            # 近期净值变化幅度（代理份额估算）
            nav_change_7d = None
            if len(nav_list) >= 8:
                nav_change_7d = (nav_list[-1] - nav_list[-8]) / nav_list[-8] * 100 if nav_list[-8] > 0 else None

            # 最新净值
            current_nav = nav_list[-1] if nav_list else None
            latest_date = date_list[-1] if date_list else ""

            # 浮盈
            profit_pct = (current_nav - cost) / cost * 100 if current_nav and cost and cost > 0 else None
            profit_amt = (current_nav - cost) * shares_held if current_nav and cost and shares_held else None

            # 份额信号
            share_signal = "⚪"
            share_note = "无份额数据"
            if spot_shares is not None:
                share_note = f"当前份额: {spot_shares/1e8:.2f}亿份"
                share_signal = "⚪"

            results.append({
                "code":          code,
                "name":          name,
                "current_nav":   round(current_nav, 4) if current_nav else None,
                "latest_date":   latest_date,
                "nav_change_7d": round(nav_change_7d, 2) if nav_change_7d is not None else None,
                "spot_shares":   spot_shares,
                "spot_mkt_val":  spot_mkt_val,
                "cost":          cost,
                "profit_pct":    round(profit_pct, 2) if profit_pct is not None else None,
                "profit_amt":    round(profit_amt, 2) if profit_amt is not None else None,
                "share_signal":  share_signal,
                "share_note":    share_note,
            })

        except Exception as e:
            results.append({
                "code":         code,
                "name":         name,
                "current_nav":  None,
                "latest_date":  "",
                "nav_change_7d": None,
                "spot_shares":  None,
                "spot_mkt_val": None,
                "cost":         cost,
                "profit_pct":   None,
                "profit_amt":   None,
                "share_signal": "⚪",
                "share_note":   f"数据获取失败: {str(e)[:40]}",
            })

    return results


def _get_column_value(row: pd.Series, possible_names: List[str]) -> Optional[float]:
    """
    动态检测列名，兼容不同数据源的列名变体
    
    Args:
        row: pandas Series 数据行
        possible_names: 可能的列名列表（按优先级排序）
    
    Returns:
        找到的第一个非空值，如果都没找到则返回 None
    """
    for name in possible_names:
        if name in row.index:
            val = _safe_float(row.get(name))
            if val is not None:
                return val
    return None


def _try_akshare_flow(top_n: int = 10) -> Dict:
    """
    尝试从 AkShare 获取 ETF 资金流向数据（带重试机制）
    
    Returns:
        成功返回 {"success": True, "data": {...}}
        失败返回 {"success": False, "error": "..."}
    """
    if not HAS_AKSHARE:
        return {"success": False, "error": "AkShare 未安装"}
    
    try:
        print("⏳ 正在通过 AkShare 获取 ETF 行情数据...")
        df = ak.fund_etf_spot_em()
        
        if df is None or df.empty:
            return {"success": False, "error": "AkShare 返回空数据"}
        
        # 动态检测列名（兼容不同版本）
        # ✅ AkShare实际返回的列名包含连字符（如"主力净流入-净额"）
        flow_col_options = ["主力净流入-净额", "主力净流入净额", "净流入", "主力净流入", "净额"]
        flow_pct_col_options = ["主力净流入-净占比", "主力净流入净占比", "净流入占比", "净占比"]
        share_col_options = ["最新份额", "基金份额", "份额", "总份额"]
        mktval_col_options = ["流通市值", "基金市值", "市值", "总市值"]
        price_col_options = ["最新价", "收盘价", "现价", "价格"]
        change_col_options = ["涨跌幅", "涨幅", "涨跌"]
        amount_col_options = ["成交额", "成交金额", "成交总量"]
        
        # 清洗数值列
        for col_options in [flow_col_options, flow_pct_col_options, share_col_options, 
                            mktval_col_options, change_col_options, amount_col_options]:
            for col in col_options:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        
        df = df.dropna(subset=["代码", "名称"])
        
        # ── 1. 主力净流入 TOP
        flow_col = None
        for col in flow_col_options:
            if col in df.columns:
                flow_col = col
                break
        
        top_inflow = []
        top_outflow = []
        
        if flow_col:
            df_flow = df.dropna(subset=[flow_col])
            if not df_flow.empty:
                # 准备输出列
                output_cols = ["代码", "名称"]
                for col in price_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                for col in change_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                output_cols.extend([flow_col])
                for col in flow_pct_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                for col in amount_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                
                top_inflow = df_flow.nlargest(top_n, flow_col)[output_cols].to_dict("records")
                top_outflow = df_flow.nsmallest(top_n, flow_col)[output_cols].to_dict("records")
        
        # ── 2. 按类别汇总主力净流入
        category_flow = {}
        for _, row in df.iterrows():
            name = str(row.get("名称", ""))
            cat = _classify_etf(name)
            
            # 动态获取资金流向值
            flow = _get_column_value(row, flow_col_options) or 0.0
            mkt_val = _get_column_value(row, mktval_col_options) or 0.0
            
            if cat not in category_flow:
                category_flow[cat] = {"inflow": 0.0, "count": 0, "mkt_val": 0.0}
            category_flow[cat]["inflow"] += flow
            category_flow[cat]["count"] += 1
            category_flow[cat]["mkt_val"] += mkt_val
        
        # 排序：主力净流入从大到小
        category_flow_sorted = sorted(
            [{"category": k, **v} for k, v in category_flow.items()],
            key=lambda x: -x["inflow"]
        )
        
        # ── 3. 流通市值 TOP
        mktval_col = None
        for col in mktval_col_options:
            if col in df.columns:
                mktval_col = col
                break
        
        share_col = None
        for col in share_col_options:
            if col in df.columns:
                share_col = col
                break
        
        top_by_mktval = []
        if mktval_col and share_col:
            df_share = df.dropna(subset=[mktval_col, share_col])
            if not df_share.empty:
                output_cols = ["代码", "名称", mktval_col, share_col]
                for col in price_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                for col in change_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                if flow_col:
                    output_cols.append(flow_col)
                
                top_by_mktval = df_share.nlargest(top_n, mktval_col)[output_cols].to_dict("records")
        
        # ── 4. 今日成交额 TOP
        amount_col = None
        for col in amount_col_options:
            if col in df.columns:
                amount_col = col
                break
        
        top_by_amount = []
        if amount_col:
            df_vol = df.dropna(subset=[amount_col])
            if not df_vol.empty:
                output_cols = ["代码", "名称", amount_col]
                for col in price_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                for col in change_col_options:
                    if col in df.columns:
                        output_cols.append(col)
                        break
                if flow_col:
                    output_cols.append(flow_col)
                
                top_by_amount = df_vol.nlargest(top_n, amount_col)[output_cols].to_dict("records")
        
        # ── 汇总统计
        total_inflow = df[flow_col].sum() if flow_col and flow_col in df.columns else 0
        total_etf_count = len(df)
        inflow_count = int((df[flow_col] > 0).sum()) if flow_col and flow_col in df.columns else 0
        outflow_count = int((df[flow_col] < 0).sum()) if flow_col and flow_col in df.columns else 0
        
        return {
            "success": True,
            "data": {
                "summary": {
                    "total_etf_count":  total_etf_count,
                    "total_inflow":     round(total_inflow, 0) if total_inflow else 0,
                    "inflow_count":     inflow_count,
                    "outflow_count":    outflow_count,
                },
                "top_inflow":          top_inflow,
                "top_outflow":         top_outflow,
                "category_flow":       category_flow_sorted,
                "top_by_mktval":       top_by_mktval,
                "top_by_amount":       top_by_amount,
            }
        }
    
    except Exception as e:
        return {"success": False, "error": f"AkShare 接口调用失败: {str(e)}"}


def _scrape_eastmoney_flow(top_n: int = 10, min_total: int = 1000) -> Dict:
    """
    从东方财富API获取 ETF 资金流向数据（备选方案）
    
    数据源: 东方财富HTTP API
    关键发现：按fid=f62（主力净流入）排序时，API返回完整的资金流向数据
    
    Args:
        top_n: 每个TOP榜单的数量
        min_total: 最小获取总数（使用分页机制）
    
    Returns:
        成功返回 {"success": True, "data": {...}}
        失败返回 {"success": False, "error": "..."}
    """
    if not HAS_REQUESTS:
        return {"success": False, "error": "requests 库未安装，无法执行API请求"}
    
    # 东方财富ETF资金流向API（按主力净流入排序）
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
        "Accept": "*/*",
    }
    
    # 分页获取数据，直到达到min_total
    all_etf_data = []
    page_num = 1
    page_size = 200  # 每页200条
    
    print(f"⏳ 正在通过东方财富API获取 ETF 资金流向数据（目标≥{min_total}条）...")
    
    try:
        while len(all_etf_data) < min_total:
            params = {
                "pn": page_num,
                "pz": page_size,
                "po": 1,
                "np": 1,
                "fltt": 2,
                "invt": 2,
                "fid": "f62",  # ✅ 关键：按主力净流入排序
                "fs": "b:MK0404",  # ETF分类
                "fields": "f1,f2,f3,f4,f5,f6,f7,f12,f13,f14,f15,f16,f17,f18,f62,f63,f64,f65,f66,f67,f68,f69,f70,f71,f72"
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code != 200:
                break  # 停止分页
            
            data = response.json()
            
            if data.get("data") is None or data["data"].get("diff") is None:
                break  # 没有更多数据
            
            # 解析当前页数据
            etf_list = data["data"]["diff"]
            
            for item in etf_list:
                try:
                    all_etf_data.append({
                        "代码": str(item.get("f12", "")),
                        "名称": str(item.get("f14", "")),
                        "最新价": _safe_float(item.get("f2")),
                        "涨跌幅": _safe_float(item.get("f3")),
                        "涨跌额": _safe_float(item.get("f4")),
                        "成交量": _safe_float(item.get("f5")),
                        "成交额": _safe_float(item.get("f6")),
                        "振幅": _safe_float(item.get("f7")),
                        "最高": _safe_float(item.get("f15")),
                        "最低": _safe_float(item.get("f16")),
                        "今开": _safe_float(item.get("f17")),
                        "昨收": _safe_float(item.get("f18")),
                        # ✅ 关键资金流向字段
                        "主力净流入净额": _safe_float(item.get("f62")),  # 主力净流入（元）
                        "主力净流入占比": _safe_float(item.get("f67")),  # 主力净流入占比（%）
                    })
                except:
                    continue
            
            # 如果返回数据少于page_size，说明已到最后一页
            if len(etf_list) < page_size:
                break
            
            page_num += 1
        
        if not all_etf_data:
            return {"success": False, "error": "解析数据失败，未提取到有效数据"}
        
        print(f"✅ 成功获取 {len(all_etf_data)} 只ETF数据")
        
        # 转换为DataFrame处理
        df = pd.DataFrame(all_etf_data)
        
        # 按主力净流入排序
        df_sorted = df.sort_values("主力净流入净额", ascending=False)
        
        # 分类汇总（删除mkt_val字段，API不返回）
        category_flow = {}
        for _, row in df.iterrows():
            name = str(row.get("名称", ""))
            cat = _classify_etf(name)
            
            flow = _safe_float(row.get("主力净流入净额")) or 0.0
            
            if cat not in category_flow:
                category_flow[cat] = {"inflow": 0.0, "count": 0}
            category_flow[cat]["inflow"] += flow
            category_flow[cat]["count"] += 1
        
        # 排序：主力净流入从大到小
        category_flow_sorted = sorted(
            [{"category": k, **v} for k, v in category_flow.items()],
            key=lambda x: -x["inflow"]
        )
        
        # 计算净流入ETF数量
        inflow_count = int((df["主力净流入净额"] > 0).sum()) if "主力净流入净额" in df.columns else 0
        outflow_count = int((df["主力净流入净额"] < 0).sum()) if "主力净流入净额" in df.columns else 0
        total_inflow = df["主力净流入净额"].sum() if "主力净流入净额" in df.columns else 0
        
        # 构建返回结构
        return {
            "success": True,
            "data": {
                "summary": {
                    "total_etf_count": len(df),
                    "total_inflow": round(total_inflow, 0) if total_inflow else 0,
                    "inflow_count": inflow_count,
                    "outflow_count": outflow_count,
                },
                "top_inflow": df_sorted.head(top_n).to_dict("records"),
                "top_outflow": df_sorted.tail(top_n).to_dict("records"),
                "category_flow": category_flow_sorted,
                "top_by_mktval": [],  # API不包含市值数据
                "top_by_amount": df.sort_values("成交额", ascending=False).head(top_n).to_dict("records"),
                "data_source": "eastmoney_http_api",
            }
        }
    
    except requests.Timeout:
        return {"success": False, "error": "API请求超时（30秒）"}
    except requests.RequestException as e:
        return {"success": False, "error": f"网络请求失败: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"数据解析失败: {str(e)}"}


def get_market_etf_flow(top_n: int = 10) -> Dict:
    """
    获取全市场 ETF 资金流向（双重数据源保障）
    
    优先使用 AkShare 接口，失败后降级到网页抓取
    
    返回结构：
    {
        "summary": {
            "total_etf_count": int,
            "total_inflow": float,
            "inflow_count": int,
            "outflow_count": int
        },
        "top_inflow": [...],      # 净流入TOP N
        "top_outflow": [...],     # 净流出TOP N
        "category_flow": [...],   # 按类别汇总
        "top_by_mktval": [...],   # 市值最大TOP N
        "top_by_amount": [...],   # 成交额最大TOP N
        "data_source": str        # 数据来源标识
    }
    """
    # 尝试 AkShare 接口
    result = _try_akshare_flow(top_n)
    
    if result.get("success"):
        data = result["data"]
        data["data_source"] = "akshare_api"
        return data
    
    # AkShare 失败，尝试网页抓取
    print(f"⚠️  AkShare 失败: {result.get('error')}")
    print("🔄 尝试备选方案：网页抓取...")
    
    result = _scrape_eastmoney_flow(top_n)
    
    if result.get("success"):
        data = result["data"]
        data["data_source"] = "eastmoney_web_scraping"
        return data
    
    # 所有方案都失败
    print(f"❌ 网页抓取失败: {result.get('error')}")
    return {"error": f"所有数据源均失败。AkShare: {result.get('error')}"}



def print_portfolio_share_flow(flow_list: List[Dict]):
    """打印持仓ETF份额变化报告"""
    print("\n📦 我的持仓 ETF — 一级市场份额快报")
    print("=" * 68)
    if not flow_list:
        print("  持仓为空，请先配置 my_etf_portfolio.json")
        return

    for f in flow_list:
        code       = f.get("code", "")
        name       = f.get("name", "")
        nav        = f.get("current_nav")
        nav_str    = f"{nav:.4f}" if nav else "N/A"
        date       = f.get("latest_date", "")
        nav_7d     = f.get("nav_change_7d")
        nav_7d_s   = f"{nav_7d:+.2f}%" if nav_7d is not None else "N/A"
        shares     = f.get("spot_shares")
        mkt_val    = f.get("spot_mkt_val")
        shares_s   = f"{shares/1e8:.2f}亿份" if shares else "N/A"
        mkt_val_s  = f"{mkt_val/1e8:.2f}亿" if mkt_val else "N/A"
        profit_pct = f.get("profit_pct")
        profit_amt = f.get("profit_amt")
        p_str      = f"{profit_pct:+.2f}%" if profit_pct is not None else "N/A"
        a_str      = f"{profit_amt:+.0f}元" if profit_amt is not None else ""

        sig = "🟢" if (profit_pct or 0) > 0 else ("🔴" if (profit_pct or 0) < 0 else "⚪")
        print(f"  {sig} [{code}] {name}")
        print(f"       净值: {nav_str} ({date})  近7日净值: {nav_7d_s}")
        print(f"       基金总份额: {shares_s}  流通市值: {mkt_val_s}")
        print(f"       我的浮盈: {p_str} {a_str}")
        print()


def print_market_etf_flow(flow: Dict):
    """打印全市场ETF资金流向报告"""
    if not flow or "error" in flow:
        print(f"\n❌ 全市场资金流向获取失败: {flow.get('error','')}")
        return

    summary = flow.get("summary", {})
    print("\n🌊 全市场 ETF 资金流向")
    print("=" * 68)
    total_inflow = summary.get("total_inflow", 0)
    total_str = _fmt_amount(total_inflow) if total_inflow else "N/A"
    inflow_c = summary.get("inflow_count", 0)
    outflow_c = summary.get("outflow_count", 0)
    total_c = summary.get("total_etf_count", 0)
    trend = "📈 净流入" if total_inflow > 0 else "📉 净流出"
    print(f"  {trend}  全市场主力净额: {total_str}  共{total_c}只ETF")
    print(f"  流入ETF: {inflow_c}只  |  流出ETF: {outflow_c}只\n")

    # ── 分类流向（删除规模字段显示）
    cat_flow = flow.get("category_flow", [])
    if cat_flow:
        print("  📊 各类ETF主力净流向排行")
        print("  " + "-" * 50)
        for i, c in enumerate(cat_flow[:12], 1):
            cat = c.get("category", "")
            inflow = c.get("inflow", 0)
            count  = c.get("count", 0)
            flow_str = _fmt_amount(inflow)
            arrow = "▲" if inflow > 0 else "▼"
            bar_len = min(int(abs(inflow) / 1e7), 15) if inflow else 0
            bar = ("█" if inflow > 0 else "░") * bar_len
            print(f"  {i:2d}. {cat:<6}  {arrow} {flow_str:<10}  {count}只  {bar}")
        print()

    # ── 主力净流入 TOP
    top_in = flow.get("top_inflow", [])
    if top_in:
        print("  💰 主力净流入 TOP 10")
        print("  " + "-" * 60)
        for i, e in enumerate(top_in, 1):
            code = str(e.get("代码", ""))
            name = str(e.get("名称", ""))[:12]
            pct   = e.get("涨跌幅")
            # ✅ 兼容两种字段名（带连字符和不带连字符）
            inflow = e.get("主力净流入-净额") or e.get("主力净流入净额")
            pct_s  = f"{pct:+.2f}%" if pct is not None else "N/A"
            flow_s = _fmt_amount(inflow) if inflow else "N/A"
            print(f"  {i:2d}. [{code}]{name:<12}  {pct_s:<8}  净流入: {flow_s}")
        print()

    # ── 主力净流出 TOP（仅当存在负值数据时显示）
    top_out = flow.get("top_outflow", [])
    if outflow_c > 0 and top_out:
        print("  💸 主力净流出 TOP 10")
        print("  " + "-" * 60)
        for i, e in enumerate(top_out, 1):
            code = str(e.get("代码", ""))
            name = str(e.get("名称", ""))[:12]
            pct   = e.get("涨跌幅")
            # ✅ 兼容两种字段名（带连字符和不带连字符）
            outflow = e.get("主力净流入-净额") or e.get("主力净流入净额")
            pct_s  = f"{pct:+.2f}%" if pct is not None else "N/A"
            flow_s = _fmt_amount(abs(outflow)) if outflow and outflow < 0 else "N/A"
            print(f"  {i:2d}. [{code}]{name:<12}  {pct_s:<8}  净流出: {flow_s}")
        print()
    elif outflow_c == 0:
        print("  ℹ️  今日暂无主力净流出数据（仅显示有净流入的LOF基金）\n")

    # ── 成交额 TOP（二级市场最活跃）
    top_amt = flow.get("top_by_amount", [])
    if top_amt:
        print("  🔥 今日成交额 TOP 10（二级市场最活跃）")
        print("  " + "-" * 60)
        for i, e in enumerate(top_amt, 1):
            code   = str(e.get("代码", ""))
            name   = str(e.get("名称", ""))[:12]
            pct    = e.get("涨跌幅")
            amt    = e.get("成交额")
            pct_s  = f"{pct:+.2f}%" if pct is not None else "N/A"
            amt_s  = _fmt_amount(amt) if amt else "N/A"
            print(f"  {i:2d}. [{code}]{name:<12}  {pct_s:<8}  成交额: {amt_s}")
        print()


# ──────────────────────────────────────────────
# 输出格式化
# ──────────────────────────────────────────────

def print_rank(rank: Dict):
    print("\n📈 今日 ETF 涨幅榜 TOP 15")
    print("=" * 65)
    if not rank.get("top_gain"):
        print("  暂无数据")
    else:
        for i, e in enumerate(rank["top_gain"], 1):
            bar = "▓" * min(int(abs(e.get("change_pct") or 0)), 20)
            print(f"  #{i:2d} [{e['code']}]{e['name']:<14}  "
                  f"{_fmt_pct(e.get('change_pct')):<8}  {bar}  "
                  f"成交:{_fmt_amount(e.get('amount'))}")

    print("\n📉 今日 ETF 跌幅榜 TOP 15")
    print("=" * 65)
    if not rank.get("top_loss"):
        print("  暂无数据")
    else:
        for i, e in enumerate(rank["top_loss"], 1):
            bar = "▒" * min(int(abs(e.get("change_pct") or 0)), 20)
            print(f"  #{i:2d} [{e['code']}]{e['name']:<14}  "
                  f"{_fmt_pct(e.get('change_pct')):<8}  {bar}  "
                  f"成交:{_fmt_amount(e.get('amount'))}")


def print_momentum(mmt: List[Dict]):
    print(f"\n🚀 ETF 动量排行（近20日涨幅，核心ETF）")
    print("=" * 65)
    if not mmt:
        print("  暂无数据（历史行情查询较慢，请稍候）")
        return
    for i, e in enumerate(mmt, 1):
        bar = "█" * min(int(abs(e.get("gain_20d") or 0) // 2), 20)
        print(f"  #{i:2d} [{e['code']}]{e['name']:<14}  "
              f"20日涨幅:{_fmt_pct(e.get('gain_20d')):<9}  "
              f"[{e['category']}]  {bar}")


def print_oversold(os_list: List[Dict]):
    print(f"\n🔍 超跌 ETF 筛选（从1年高点回撤≥25%，核心ETF）")
    print("=" * 65)
    if not os_list:
        print("  当前核心ETF中暂无回撤超过25%的标的")
        return
    for e in os_list:
        dd = e.get("drawdown", 0) * 100
        print(f"  [{e['code']}]{e['name']:<14}  "
              f"回撤:{dd:.1f}%  "
              f"现价:{e.get('price', 'N/A')}  "
              f"1年高点:{e.get('peak', 'N/A'):.3f}  "
              f"[{e['category']}]")
        # 操作提示
        if dd >= 40:
            print(f"          ⚠️  深度超跌，可关注分批低吸机会（确认基本面未恶化）")
        elif dd >= 30:
            print(f"          💡 中度超跌，可纳入观察池")


def print_core_quotes(quotes: List[Dict]):
    print(f"\n📊 核心 ETF 行情监控")
    print("=" * 65)
    if not quotes:
        print("  暂无数据")
        return
    current_cat = None
    for e in quotes:
        if e.get("category") != current_cat:
            current_cat = e.get("category")
            print(f"\n  ── {current_cat} ──")
        pct_str = _fmt_pct(e.get("change_pct"))
        emoji = "📈" if (e.get("change_pct") or 0) > 0 else ("📉" if (e.get("change_pct") or 0) < 0 else "➡️")
        price = e.get("price")
        price_str = f"{price:.3f}" if price else "N/A"
        amt_str = _fmt_amount(e.get("amount"))
        print(f"  {emoji} [{e['code']}]{e['name']:<14}  {price_str:<8}  {pct_str:<8}  成交:{amt_str}")


def print_portfolio(portfolio: List[Dict]):
    print(f"\n💼 我的 ETF 持仓报告")
    print("=" * 65)
    if not portfolio:
        print("  持仓为空。请编辑 my_etf_portfolio.json 添加持仓")
        print("  格式参考：")
        print('  {"etfs": [{"code":"510300","name":"沪深300ETF","cost_price":3.8,"shares":1000,"note":"核心仓"}]}')
        return

    total_cost = 0.0
    total_val  = 0.0

    for h in portfolio:
        price = h.get("price")
        price_str = f"{price:.4f}" if price else "N/A"
        profit_pct = h.get("profit_pct")
        profit_amt = h.get("profit_amt")
        pct_emoji = "🟢" if (profit_pct or 0) > 0 else ("🔴" if (profit_pct or 0) < 0 else "⚪")
        pct_str = _fmt_pct(profit_pct)
        amt_str = f"{profit_amt:+.2f}元" if profit_amt is not None else ""
        today_pct = _fmt_pct(h.get("change_pct"))

        print(f"  {pct_emoji} [{h['code']}]{h['name']:<14}  "
              f"现价:{price_str}  今日:{today_pct}  "
              f"浮盈:{pct_str} {amt_str}  {h.get('note','')}")

        if h.get("cost") and h.get("shares") and price:
            total_cost += h["cost"] * h["shares"]
            total_val  += price * h["shares"]

    if total_cost > 0:
        total_profit = total_val - total_cost
        total_pct = total_profit / total_cost * 100
        emoji = "🟢" if total_profit >= 0 else "🔴"
        print(f"\n  {emoji} 总持仓市值：{total_val:.2f}元  总浮盈：{total_profit:+.2f}元（{total_pct:+.2f}%）")


def print_portfolio_valuation_report(val_list: List[Dict]):
    """打印持仓 ETF 净值历史分位估值报告"""
    if not val_list:
        return
    print(f"\n  📊 我的持仓 ETF 估值快报（基于净值历史分位）")
    print("  " + "=" * 68)
    for v in val_list:
        sig       = v.get("signal", "⚪")
        code      = v.get("code", "")
        name      = v.get("name", "")
        nav       = v.get("current_nav")
        nav_str   = f"{nav:.4f}" if nav else "N/A"
        date      = v.get("latest_date", "")
        pct_all   = v.get("pct_all")
        pct_3y    = v.get("pct_3y")
        pct_all_s = f"{pct_all:.1f}%" if pct_all is not None else "N/A"
        pct_3y_s  = f"{pct_3y:.1f}%"  if pct_3y  is not None else "N/A"
        dd        = v.get("drawdown")
        dd_s      = f"{dd:.1f}%" if dd is not None else "N/A"
        cost      = v.get("cost")
        cost_pct  = v.get("cost_pct")
        cost_s    = f"{cost:.3f}" if cost else "N/A"
        cost_pct_s= f"{cost_pct:+.2f}%" if cost_pct is not None else "N/A"
        peak      = v.get("peak")
        lowest    = v.get("lowest")
        n_data    = v.get("data_count", 0)
        advice    = v.get("advice", "")

        print(f"  {sig} [{code}] {name}")
        print(f"       当前净值: {nav_str}  ({date})")
        print(f"       全历史分位: {pct_all_s}   近3年分位: {pct_3y_s}   历史高点回撤: {dd_s}")
        print(f"       成本: {cost_s}  浮盈: {cost_pct_s}")
        print(f"       历史数据: {n_data}天  最低净值: {lowest:.4f}  最高净值: {peak:.4f}")
        print(f"       💬 {advice}")
        print()


def print_valuation(val_list: List[Dict]):
    print(f"\n📐 宽基指数 PE/PB 估值参考")
    print("=" * 72)
    if not val_list:
        print("  暂无估值数据")
        return
    for v in val_list:
        pe_str  = f"{v['pe']:.2f}"  if v.get("pe")  is not None else "N/A"
        pb_str  = f"{v['pb']:.2f}"  if v.get("pb")  is not None else "N/A"
        pe_pct  = f"{v['pe_pct']:.1f}%" if v.get("pe_pct") is not None else "N/A"
        pb_pct  = f"{v['pb_pct']:.1f}%" if v.get("pb_pct") is not None else "N/A"
        sig     = v.get("pe_signal", "⚪")
        date    = v.get("date", "")
        years   = v.get("years", 10)
        print(f"  {sig} [{v['etf_code']}] {v['name']}  ({date})")
        print(f"       PE: {pe_str:<7} 近{years}年分位: {pe_pct:<8}  "
              f"PB: {pb_str:<6} 近{years}年分位: {pb_pct}")
        print(f"       💬 {v.get('advice', '')}")
        print()


# ──────────────────────────────────────────────
# 主函数
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ETF 行情扫描与分析工具")
    parser.add_argument(
        "--mode",
        choices=["rank", "momentum", "oversold", "core", "portfolio", "valuation", "share_flow", "market_flow"],
        help="单项分析模式"
    )
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args()

    if not HAS_AKSHARE:
        print("❌ akshare 未安装，请运行：pip install akshare pandas", file=sys.stderr)
        sys.exit(1)

    show_all = args.mode is None
    output: Dict[str, Any] = {}

    # ── 先获取全市场 ETF 实时行情（多个功能依赖）
    need_spot = show_all or args.mode in ("rank", "core", "portfolio")
    spot_df = pd.DataFrame()
    if need_spot:
        print("⏳ 正在获取全市场 ETF 实时行情...", file=sys.stderr)
        spot_df = get_all_etf_spot()

    # ── 涨跌幅排行
    if show_all or args.mode == "rank":
        rank = get_etf_rank(spot_df)
        output["rank"] = rank
        if not args.json:
            print_rank(rank)

    # ── 核心 ETF 监控
    if show_all or args.mode == "core":
        core_quotes = get_core_etf_quotes(spot_df)
        output["core"] = core_quotes
        if not args.json:
            print_core_quotes(core_quotes)

    # ── 用户持仓（含估值快报）
    if show_all or args.mode == "portfolio":
        portfolio = get_etf_portfolio_report(spot_df)
        output["portfolio"] = portfolio
        if not args.json:
            print_portfolio(portfolio)
        # 持仓分析后追加持仓ETF净值分位估值
        if not args.json and args.mode == "portfolio":
            print("⏳ 正在计算持仓ETF净值历史分位...", file=sys.stderr)
            val_list = get_portfolio_valuation_report()
            output["portfolio_valuation"] = val_list
            print_portfolio_valuation_report(val_list)

    # ── 动量筛选（需查历史，较慢）
    if show_all or args.mode == "momentum":
        print("⏳ 正在计算近20日动量（查历史行情，约需30秒）...", file=sys.stderr)
        mmt = get_etf_momentum()
        output["momentum"] = mmt
        if not args.json:
            print_momentum(mmt)

    # ── 超跌筛选（需查历史，较慢）
    if show_all or args.mode == "oversold":
        print("⏳ 正在计算从1年高点回撤（查历史行情，约需30秒）...", file=sys.stderr)
        os_list = get_etf_oversold()
        output["oversold"] = os_list
        if not args.json:
            print_oversold(os_list)

    # ── 估值参考
    if show_all or args.mode == "valuation":
        val_list = get_valuation_reference()
        output["valuation"] = val_list
        if not args.json:
            print_valuation(val_list)

    # ── 持仓份额变化（一级市场申购/赎回信号）
    if args.mode == "share_flow":
        print("⏳ 正在获取持仓ETF份额数据...", file=sys.stderr)
        share_flow = get_portfolio_share_flow()
        output["share_flow"] = share_flow
        if not args.json:
            print_portfolio_share_flow(share_flow)

    # ── 全市场资金流向
    if args.mode == "market_flow":
        print("⏳ 正在获取全市场ETF资金流向（东财实时数据）...", file=sys.stderr)
        market_flow = get_market_etf_flow()
        output["market_flow"] = market_flow
        if not args.json:
            print_market_etf_flow(market_flow)

    # ── 汇总摘要（全量模式）
    if show_all and not args.json:
        print("\n" + "=" * 65)
        print("📋 ETF 扫描摘要")
        print("=" * 65)
        rank = output.get("rank", {})
        top_gain = rank.get("top_gain", [])
        top_loss = rank.get("top_loss", [])
        if top_gain:
            print(f"  今日涨幅最大：[{top_gain[0]['code']}]{top_gain[0]['name']}  {_fmt_pct(top_gain[0].get('change_pct'))}")
        if top_loss:
            print(f"  今日跌幅最大：[{top_loss[0]['code']}]{top_loss[0]['name']}  {_fmt_pct(top_loss[0].get('change_pct'))}")
        oversold = output.get("oversold", [])
        if oversold:
            print(f"  超跌候选：{'  '.join([e['name'] for e in oversold[:3]])}")
        mmt = output.get("momentum", [])
        if mmt:
            print(f"  强势ETF：{'  '.join([e['name'] for e in mmt[:3]])}")
        print()

    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
