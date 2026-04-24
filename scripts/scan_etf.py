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

依赖：pip install akshare pandas

用法：
  python3 scripts/scan_etf.py                    # 全量扫描
  python3 scripts/scan_etf.py --mode rank        # 今日涨跌幅排行
  python3 scripts/scan_etf.py --mode momentum    # 动量筛选（趋势ETF）
  python3 scripts/scan_etf.py --mode oversold    # 超跌筛选（低吸候选）
  python3 scripts/scan_etf.py --mode core        # 核心ETF监控
  python3 scripts/scan_etf.py --mode portfolio   # 我的ETF持仓
  python3 scripts/scan_etf.py --mode valuation   # 估值参考
  python3 scripts/scan_etf.py --json             # JSON格式输出
"""

import json
import sys
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Optional, List, Dict, Any

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
        choices=["rank", "momentum", "oversold", "core", "portfolio", "valuation"],
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
