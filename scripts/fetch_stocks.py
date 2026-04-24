#!/usr/bin/env python3
"""
fetch_stocks.py — 炒股 Skill 数据获取脚本
==========================================
功能：
  1. 获取 A 股各行业龙头股票列表（基于 akshare 数据）
  2. 读取用户自选股（my_portfolio.json）
  3. 输出 JSON 供 Skill 消费

依赖安装：
  pip install akshare pandas

用法：
  python scripts/fetch_stocks.py                  # 获取全部（龙头 + 自选）
  python scripts/fetch_stocks.py --leaders        # 仅获取行业龙头
  python scripts/fetch_stocks.py --portfolio      # 仅获取用户自选股实时行情
  python scripts/fetch_stocks.py --industry 白酒   # 获取指定行业龙头
"""

import json
import sys
import os
import argparse
from datetime import datetime, time
from pathlib import Path
from typing import Optional, List, Dict

# ── 根目录（脚本所在 scripts/ 的上一层）
ROOT_DIR = Path(__file__).parent.parent
PORTFOLIO_FILE = ROOT_DIR / "my_portfolio.json"
CACHE_FILE = ROOT_DIR / ".cache" / "leaders_cache.json"
CACHE_TTL_HOURS = 24  # 行业龙头缓存 24 小时


# ──────────────────────────────────────────────
# 1. A 股各行业龙头（内置静态列表 + akshare 动态补充）
# ──────────────────────────────────────────────

# 静态兜底列表：各行业公认龙头（code 为 A 股代码）
INDUSTRY_LEADERS_STATIC = {
    "白酒": [
        {"code": "600519", "name": "贵州茅台", "market": "SH"},
        {"code": "000858", "name": "五粮液",   "market": "SZ"},
        {"code": "000568", "name": "泸州老窖", "market": "SZ"},
        {"code": "002304", "name": "洋河股份", "market": "SZ"},
    ],
    "银行": [
        {"code": "601398", "name": "工商银行", "market": "SH"},
        {"code": "601288", "name": "农业银行", "market": "SH"},
        {"code": "600036", "name": "招商银行", "market": "SH"},
        {"code": "601328", "name": "交通银行", "market": "SH"},
        {"code": "002142", "name": "宁波银行", "market": "SZ"},
    ],
    "新能源 / 动力电池": [
        {"code": "300750", "name": "宁德时代", "market": "SZ"},
        {"code": "002594", "name": "比亚迪",   "market": "SZ"},
        {"code": "601012", "name": "隆基绿能", "market": "SH"},
        {"code": "688599", "name": "天合光能", "market": "SH"},
    ],
    "半导体 / 芯片": [
        {"code": "688981", "name": "中芯国际", "market": "SH"},
        {"code": "002049", "name": "紫光国微", "market": "SZ"},
        {"code": "603501", "name": "韦尔股份", "market": "SH"},
        {"code": "688012", "name": "中微公司", "market": "SH"},
        {"code": "002371", "name": "北方华创", "market": "SZ"},
    ],
    "医药 / 创新药": [
        {"code": "600276", "name": "恒瑞医药", "market": "SH"},
        {"code": "000661", "name": "长春高新", "market": "SZ"},
        {"code": "688180", "name": "君实生物", "market": "SH"},
        {"code": "002252", "name": "上海莱士", "market": "SZ"},
    ],
    "医疗器械": [
        {"code": "300760", "name": "迈瑞医疗", "market": "SZ"},
        {"code": "688271", "name": "联影医疗", "market": "SH"},
        {"code": "300015", "name": "爱尔眼科", "market": "SZ"},
    ],
    "消费电子 / 手机产业链": [
        {"code": "002415", "name": "海康威视", "market": "SZ"},
        {"code": "002352", "name": "顺丰控股", "market": "SZ"},
        {"code": "300474", "name": "景旺电子", "market": "SZ"},
        {"code": "002241", "name": "歌尔股份", "market": "SZ"},
    ],
    "互联网 / 软件": [
        {"code": "688111", "name": "金山办公", "market": "SH"},
        {"code": "300033", "name": "同花顺",   "market": "SZ"},
        {"code": "603888", "name": "新华网",   "market": "SH"},
        {"code": "688029", "name": "南微医学", "market": "SH"},
    ],
    "军工 / 国防": [
        {"code": "600760", "name": "中航沈飞", "market": "SH"},
        {"code": "002179", "name": "中航光电", "market": "SZ"},
        {"code": "600893", "name": "航发动力", "market": "SH"},
        {"code": "688122", "name": "西部超导", "market": "SH"},
    ],
    "煤炭 / 能源": [
        {"code": "601088", "name": "中国神华", "market": "SH"},
        {"code": "600188", "name": "兖矿能源", "market": "SH"},
        {"code": "601225", "name": "陕西煤业", "market": "SH"},
    ],
    "钢铁 / 有色金属": [
        {"code": "600519", "name": "宝钢股份", "market": "SH"},
        {"code": "601600", "name": "中国铝业", "market": "SH"},
        {"code": "600585", "name": "海螺水泥", "market": "SH"},
        {"code": "002460", "name": "赣锋锂业", "market": "SZ"},
        {"code": "002466", "name": "天齐锂业", "market": "SZ"},
    ],
    "房地产": [
        {"code": "000002", "name": "万科A",    "market": "SZ"},
        {"code": "600048", "name": "保利发展", "market": "SH"},
        {"code": "001979", "name": "招商蛇口", "market": "SZ"},
    ],
    "保险 / 券商": [
        {"code": "601318", "name": "中国平安", "market": "SH"},
        {"code": "600030", "name": "中信证券", "market": "SH"},
        {"code": "601688", "name": "华泰证券", "market": "SH"},
        {"code": "601601", "name": "中国太保", "market": "SH"},
    ],
    "零售 / 消费": [
        {"code": "600132", "name": "重庆啤酒", "market": "SH"},
        {"code": "603288", "name": "海天味业", "market": "SH"},
        {"code": "000895", "name": "双汇发展", "market": "SZ"},
        {"code": "002414", "name": "高德红外", "market": "SZ"},
    ],
    "人工智能 / AI": [
        {"code": "002230", "name": "科大讯飞", "market": "SZ"},
        {"code": "300529", "name": "健帆生物", "market": "SZ"},
        {"code": "688041", "name": "海光信息", "market": "SH"},
        {"code": "300142", "name": "沃森生物", "market": "SZ"},
    ],
    "汽车整车": [
        {"code": "002594", "name": "比亚迪",   "market": "SZ"},
        {"code": "600104", "name": "上汽集团", "market": "SH"},
        {"code": "000625", "name": "长安汽车", "market": "SZ"},
        {"code": "601238", "name": "广汽集团", "market": "SH"},
    ],
    "化工": [
        {"code": "600309", "name": "万华化学", "market": "SH"},
        {"code": "002648", "name": "卫星化学", "market": "SZ"},
        {"code": "600160", "name": "巨化股份", "market": "SH"},
    ],
    "农业 / 养殖": [
        {"code": "002714", "name": "牧原股份", "market": "SZ"},
        {"code": "000876", "name": "新希望",   "market": "SZ"},
        {"code": "600438", "name": "通威股份", "market": "SH"},
    ],
}


def load_cache():
    """读取缓存（24小时内有效）"""
    if not CACHE_FILE.exists():
        return None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_at = datetime.fromisoformat(data.get("cached_at", "2000-01-01"))
        age_hours = (datetime.now() - cached_at).total_seconds() / 3600
        if age_hours < CACHE_TTL_HOURS:
            return data.get("leaders")
    except Exception:
        pass
    return None


def save_cache(leaders: dict):
    """保存缓存"""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"cached_at": datetime.now().isoformat(), "leaders": leaders}, f, ensure_ascii=False, indent=2)


def is_trading_time() -> bool:
    """
    判断当前是否处于 A 股交易时段：
      工作日（周一至周五）09:30 - 15:00
      不考虑法定节假日（节假日极少数情况可手动处理）
    """
    now = datetime.now()
    if now.weekday() >= 5:          # 周六=5, 周日=6
        return False
    t = now.time()
    return time(9, 30) <= t <= time(15, 0)


def _safe_float(val) -> Optional[float]:
    """安全转换为 float，失败或为 0/NaN 返回 None"""
    try:
        v = float(val)
        return v if v != 0 else None
    except (TypeError, ValueError):
        return None


def fetch_realtime_quotes(codes: List[str], market_map: Dict[str, str] = None) -> Dict:
    """
    根据当前时间智能选择行情接口：
    - 交易时间：新浪分钟线（实时价）
    - 非交易时间：新浪 ETF 日线 / 股票日线（收盘价）

    market_map: {code: "SH"|"SZ"} 可选，不传则按代码前缀自动推断
    """
    try:
        import akshare as ak
    except ImportError:
        print("⚠️  akshare 未安装，跳过行情获取。运行: pip install akshare", file=sys.stderr)
        return {}

    result = {}
    if market_map is None:
        market_map = {}

    def _to_sina(code: str) -> str:
        mkt = market_map.get(code, "")
        if mkt.upper() == "SH":
            return f"sh{code}"
        if mkt.upper() == "SZ":
            return f"sz{code}"
        # 自动推断：5/6 开头 → 沪市，其余 → 深市
        return f"sh{code}" if code.startswith(("5", "6")) else f"sz{code}"

    def _is_etf(code: str) -> bool:
        """ETF 代码特征：51xxxx / 15xxxx / 51xxxx / 56xxxx 等"""
        return code.startswith(("51", "15", "56", "58", "16", "50"))

    if is_trading_time():
        print("📡 交易时间，获取实时行情（新浪分钟线）…", file=sys.stderr)
        for code in codes:
            sina_sym = _to_sina(code)
            try:
                df_m = ak.stock_zh_a_minute(symbol=sina_sym, period="5", adjust="")
                if df_m is None or df_m.empty:
                    continue
                df_m = df_m.sort_values("day")
                last = df_m.iloc[-1]
                price = _safe_float(last.get("close"))
                open_ = _safe_float(last.get("open"))
                high  = _safe_float(last.get("high"))
                low   = _safe_float(last.get("low"))
                trade_time = str(last.get("day", ""))[:16]

                # 昨收：ETF 用 fund_etf_hist_sina，个股用 stock_zh_a_daily
                prev_close = None
                try:
                    if _is_etf(code):
                        df_d = ak.fund_etf_hist_sina(symbol=sina_sym)
                    else:
                        df_d = ak.stock_zh_a_daily(symbol=sina_sym, adjust="")
                    if df_d is not None and not df_d.empty:
                        df_d = df_d.sort_values("date")
                        prev_close = _safe_float(df_d.iloc[-1].get("close"))
                except Exception:
                    pass

                change_pct = None
                if price and prev_close and prev_close > 0:
                    change_pct = round((price - prev_close) / prev_close * 100, 2)

                result[code] = {
                    "data_type":  "realtime",
                    "trade_time": trade_time,
                    "price":      price,
                    "open":       open_,
                    "high":       high,
                    "low":        low,
                    "change_pct": change_pct,
                    "pe_ttm":     None,
                    "pb":         None,
                }
            except Exception as e:
                print(f"⚠️  [{sina_sym}] 实时行情获取失败: {e}", file=sys.stderr)

    else:
        print("🔔 非交易时间，获取最近交易日收盘价…", file=sys.stderr)

        for code in codes:
            sina_sym = _to_sina(code)
            try:
                if _is_etf(code):
                    df_hist = ak.fund_etf_hist_sina(symbol=sina_sym)
                    date_col, close_col = "date", "close"
                else:
                    df_hist = ak.stock_zh_a_daily(symbol=sina_sym, adjust="")
                    date_col, close_col = "date", "close"

                if df_hist is None or df_hist.empty:
                    continue
                df_hist = df_hist.sort_values(date_col)
                last = df_hist.iloc[-1]
                close = _safe_float(last.get(close_col))
                change_pct = None
                if len(df_hist) >= 2:
                    pc2 = _safe_float(df_hist.iloc[-2].get(close_col))
                    if close and pc2 and pc2 > 0:
                        change_pct = round((close - pc2) / pc2 * 100, 2)

                result[code] = {
                    "data_type":  "close",
                    "trade_date": str(last.get(date_col, ""))[:10],
                    "close":      close,
                    "open":       _safe_float(last.get("open")),
                    "high":       _safe_float(last.get("high")),
                    "low":        _safe_float(last.get("low")),
                    "change_pct": change_pct,
                    "pe_ttm":     None,
                    "pb":         None,
                }
            except Exception as e:
                print(f"⚠️  [{sina_sym}] 收盘价获取失败: {e}", file=sys.stderr)

    return result


def get_industry_leaders(industry_filter: str = None) -> dict:
    """
    获取行业龙头列表（静态列表 + akshare 动态行情）
    industry_filter: 若指定则只返回该行业
    """
    # 优先走缓存
    cached = load_cache()
    leaders = cached if cached else INDUSTRY_LEADERS_STATIC

    # 按行业过滤
    if industry_filter:
        matched = {k: v for k, v in leaders.items() if industry_filter in k}
        if not matched:
            # 模糊匹配
            matched = {k: v for k, v in leaders.items()
                       if any(industry_filter.lower() in k.lower() for _ in [1])}
        leaders = matched if matched else leaders

    # 收集所有 code 批量拉行情
    all_codes = [s["code"] for stocks in leaders.values() for s in stocks]
    quotes = fetch_realtime_quotes(all_codes)

    # 合并行情到结果
    result = {}
    for industry, stocks in leaders.items():
        result[industry] = []
        for s in stocks:
            q = quotes.get(s["code"], {})
            result[industry].append({
                **s,
                **{k: v for k, v in q.items()},   # 合并全部行情字段
            })

    # 更新缓存（仅全量时）
    if not industry_filter:
        save_cache(INDUSTRY_LEADERS_STATIC)

    return result


def get_portfolio() -> list:
    """
    读取用户自选股（my_portfolio.json）并附加实时行情
    """
    if not PORTFOLIO_FILE.exists():
        return []

    with open(PORTFOLIO_FILE, "r", encoding="utf-8") as f:
        portfolio = json.load(f)

    stocks = portfolio.get("stocks", [])
    codes  = [s["code"] for s in stocks]
    mkt_map = {s["code"]: s.get("market", "") for s in stocks}
    quotes = fetch_realtime_quotes(codes, market_map=mkt_map)

    result = []
    for s in stocks:
        q = quotes.get(s["code"], {})
        entry = {**s, **{k: v for k, v in q.items()}}  # 合并全部行情字段
        # 计算浮动盈亏（如果有成本价和当前/收盘价）
        cur_price = entry.get("price") or entry.get("close")
        cost = entry.get("cost_price")
        shares = entry.get("shares")
        if cur_price and cost:
            entry["profit_pct"]    = round((cur_price - cost) / cost * 100, 2)
            entry["profit_amount"] = round((cur_price - cost) * shares, 2) if shares else None
        result.append(entry)
    return result


def format_stock_line(s: dict) -> str:
    """格式化单条股票输出，根据 data_type 区分交易时/收盘后"""
    data_type = s.get("data_type", "")

    if data_type == "realtime":
        # 交易时间：显示 当前价 / 开盘 / 最高 / 最低
        price  = f"现价 ¥{s['price']:.2f}"   if s.get("price") else "N/A"
        ohlc   = ""
        if s.get("open") and s.get("high") and s.get("low"):
            ohlc = f"开{s['open']:.2f} 高{s['high']:.2f} 低{s['low']:.2f}"
        change = f"{s['change_pct']:+.2f}%" if s.get("change_pct") is not None else ""
        pe     = f"PE={s['pe_ttm']:.1f}"    if s.get("pe_ttm") else ""
        pb     = f"PB={s['pb']:.2f}"        if s.get("pb")     else ""
        meta   = "  ".join(filter(None, [price, change, ohlc, pe, pb]))
    elif data_type == "close":
        # 非交易时间：显示 收盘价 / 开盘 / 最高 / 最低 + 交易日期
        trade_date = s.get("trade_date", "")
        close  = f"收盘 ¥{s['close']:.2f}"  if s.get("close") else "N/A"
        ohlc   = ""
        if s.get("open") and s.get("high") and s.get("low"):
            ohlc = f"开{s['open']:.2f} 高{s['high']:.2f} 低{s['low']:.2f}"
        change = f"{s['change_pct']:+.2f}%" if s.get("change_pct") is not None else ""
        pe     = f"PE={s['pe_ttm']:.1f}"    if s.get("pe_ttm") else ""
        pb     = f"PB={s['pb']:.2f}"        if s.get("pb")     else ""
        date_str = f"[{trade_date}]"        if trade_date      else ""
        meta   = "  ".join(filter(None, [close, change, ohlc, pe, pb, date_str]))
    else:
        # 降级：无行情数据
        meta = "N/A"

    # 浮动盈亏（自选股持仓）
    profit = ""
    if s.get("profit_pct") is not None:
        sign = "+" if s["profit_pct"] >= 0 else ""
        profit = f"  浮盈 {sign}{s['profit_pct']:.2f}%"
        if s.get("profit_amount") is not None:
            profit += f"（{sign}{s['profit_amount']:.0f}元）"

    return f"  [{s['code']}] {s['name']}  {meta}{profit}"


def print_leaders(leaders: dict):
    print("\n📊 A股各行业龙头股票")
    print("=" * 60)
    for industry, stocks in leaders.items():
        print(f"\n▸ {industry}")
        for s in stocks:
            print(format_stock_line(s))
    print()


def print_portfolio(portfolio: list):
    if not portfolio:
        print("\n📁 未找到自选股（请编辑 my_portfolio.json）\n")
        return
    print("\n📁 我的自选股")
    print("=" * 60)
    for s in portfolio:
        note = f"  # {s['note']}" if s.get("note") else ""
        print(format_stock_line(s) + note)
    print()


def main():
    parser = argparse.ArgumentParser(description="炒股 Skill 数据获取工具")
    parser.add_argument("--leaders",   action="store_true", help="只获取行业龙头")
    parser.add_argument("--portfolio", action="store_true", help="只获取自选股行情")
    parser.add_argument("--industry",  type=str,            help="指定行业（如 白酒）")
    parser.add_argument("--json",      action="store_true", help="以 JSON 格式输出（供程序消费）")
    args = parser.parse_args()

    show_all = not args.leaders and not args.portfolio

    output = {}

    if args.leaders or show_all:
        leaders = get_industry_leaders(args.industry)
        output["industry_leaders"] = leaders
        if not args.json:
            print_leaders(leaders)

    if args.portfolio or show_all:
        portfolio = get_portfolio()
        output["my_portfolio"] = portfolio
        if not args.json:
            print_portfolio(portfolio)

    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
