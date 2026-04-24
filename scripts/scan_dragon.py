#!/usr/bin/env python3
"""
scan_dragon.py — NGA 龙头战法量化扫股脚本
==========================================
基于「交易猿」龙头战法，对 A 股全市场进行量化扫描，输出：

1. 【情绪判断】当日市场情绪阶段（冰点/复苏/正常/高潮）
2. 【涨停板池】当日涨停股池 + 连板高度 + 市值
3. 【首板候选】今日首板（近10日内无涨停记录的涨停股）
4. 【连板龙头候选】近N日连续涨停、高度前3
5. 【首阴买点】曾经连板、今日首次出现阴线且缩量
6. 【板块热度】今日涨停家数最多的板块（主线判断）

依赖：pip install akshare pandas

用法：
  python scripts/scan_dragon.py                  # 全量扫描
  python scripts/scan_dragon.py --mode emotion   # 仅看情绪
  python scripts/scan_dragon.py --mode zt        # 涨停板池
  python scripts/scan_dragon.py --mode firstzt   # 首板候选
  python scripts/scan_dragon.py --mode lianban   # 连板龙头
  python scripts/scan_dragon.py --mode firstyin  # 首阴买点
  python scripts/scan_dragon.py --json           # JSON格式输出
"""

import json
import sys
import argparse
from datetime import datetime, date, timedelta, time
from pathlib import Path
from typing import Optional, List, Dict, Any

try:
    import akshare as ak
    import pandas as pd
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False


# ── 情绪阈值（龙头战法标准）
EMOTION_BOOM   = 3500   # 高潮：普涨
EMOTION_COLD   = 1000   # 冰点：普跌
EMOTION_FREEZE = 500    # 极端冰点


def is_trading_time() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return time(9, 30) <= t <= time(15, 0)


def _safe_float(val) -> Optional[float]:
    try:
        v = float(val)
        return v if (v == v) else None   # NaN 检测
    except (TypeError, ValueError):
        return None


# ──────────────────────────────────────────────
# 1. 情绪判断
# ──────────────────────────────────────────────

def _get_last_trading_date() -> str:
    """获取最近的交易日（往前推，跳过周末）"""
    d = datetime.now()
    # 如果是交易时段内，今天就是交易日
    if is_trading_time():
        return d.strftime("%Y%m%d")
    # 非交易时段往前找，最多找7天
    for i in range(7):
        candidate = d - timedelta(days=i)
        if candidate.weekday() < 5:  # 0=Mon ... 4=Fri
            return candidate.strftime("%Y%m%d")
    return d.strftime("%Y%m%d")


def get_emotion_data(zt_pool: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    获取市场情绪数据。
    - 交易时段：调用 stock_zh_a_spot_em() 实时全市场行情统计
    - 非交易时段：从涨停板池（已获取）+ 跌停板池推算，避免断连

    参数 zt_pool：若已获取涨停板池数据，可直接传入避免重复查询。
    """
    if not HAS_AKSHARE:
        return {"error": "akshare 未安装"}

    result: Dict[str, Any] = {}

    if is_trading_time():
        # ── 交易时段：用实时行情统计
        try:
            df = ak.stock_zh_a_spot_em()
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")

            rise_count = int((df["涨跌幅"] > 0).sum())
            fall_count = int((df["涨跌幅"] < 0).sum())
            zt_count   = int((df["涨跌幅"] >= 9.5).sum())
            dt_count   = int((df["涨跌幅"] <= -9.5).sum())

            result = {
                "rise_count": rise_count,
                "fall_count": fall_count,
                "zt_count":   zt_count,
                "dt_count":   dt_count,
                "data_source": "realtime",
            }
        except Exception as e:
            result["error"] = f"实时行情获取失败: {e}"
            result["data_source"] = "failed"
    else:
        # ── 非交易时段：从涨停板池 + 跌停板池推算情绪
        trade_date = _get_last_trading_date()
        result["data_source"] = f"history({trade_date})"

        # 涨停家数：从涨停板池获取
        if zt_pool is not None:
            zt_count = len(zt_pool)
        else:
            try:
                df_zt = ak.stock_zt_pool_em(date=trade_date)
                zt_count = len(df_zt) if df_zt is not None else 0
            except Exception:
                zt_count = 0

        # 跌停家数：从跌停板池获取
        try:
            df_dt = ak.stock_dt_pool_em(date=trade_date)
            dt_count = len(df_dt) if df_dt is not None else 0
        except Exception:
            dt_count = 0

        # 非交易时段无法精确统计上涨/下跌家数，用经验估算：
        # 涨停≥50 通常上涨>3000，跌停≥50 通常下跌>3000
        # 这里用涨停/跌停作为情绪代理指标
        rise_count = None  # 无实时数据
        fall_count = None

        result.update({
            "zt_count":   zt_count,
            "dt_count":   dt_count,
            "rise_count": rise_count,
            "fall_count": fall_count,
        })

    # ── 情绪阶段判断（优先用上涨家数，无则用涨停家数代理）
    rc = result.get("rise_count")
    zt = result.get("zt_count", 0)
    dt = result.get("dt_count", 0)

    if rc is not None:
        # 实时数据：用上涨家数判断
        if rc >= EMOTION_BOOM:
            stage, tip = "高潮", "普涨日，去弱换强，卖跟风股、调仓进龙头"
        elif rc <= EMOTION_FREEZE:
            stage, tip = "极端冰点", "极端冰点！情绪压制到极致，大概率是重要转折点，重点布局"
        elif rc <= EMOTION_COLD:
            stage, tip = "冰点", "冰点日，低吸前期强势股，博弈情绪回暖；注意逆势抗跌个股"
        else:
            stage, tip = "正常", "正常波动，按盘面机动"
    else:
        # 历史数据：用涨停家数 + 跌停家数代理
        if zt >= 100 and dt <= 20:
            stage, tip = "高潮（参考）", "涨停家数多、跌停少，市场情绪偏高，注意去弱换强"
        elif dt >= 100 and zt <= 20:
            stage, tip = "冰点（参考）", "跌停家数多、涨停少，情绪低迷，关注次日低吸机会"
        elif zt >= 50:
            stage, tip = "偏暖（参考）", "涨停家数较多，市场有赚钱效应，重点跟踪主线板块"
        elif dt >= 50:
            stage, tip = "偏冷（参考）", "跌停家数较多，亏钱效应明显，控制仓位"
        else:
            stage, tip = "正常（参考）", "涨跌停家数适中，按主线板块强弱机动操作"

    result["emotion_stage"] = stage
    result["tip"] = tip
    return result


# ──────────────────────────────────────────────
# 2. 涨停板池（当日）
# ──────────────────────────────────────────────

def get_zt_pool() -> List[Dict]:
    """
    获取最近交易日涨停板股票池（东财数据）
    返回：代码、名称、连板数、涨停时间、成交金额、市值、所属板块
    """
    if not HAS_AKSHARE:
        return []

    trade_date = _get_last_trading_date()
    try:
        df = ak.stock_zt_pool_em(date=trade_date)
        if df is None or df.empty:
            return []

        # 统一列名（akshare 版本差异）
        col_map = {
            "代码": "code", "名称": "name",
            "连板数": "lianban", "涨停时间": "zt_time",
            "成交额": "amount", "流通市值": "cap",
            "所属板块": "sector", "涨跌幅": "change_pct",
            "封板资金": "sealed_amount",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        result = []
        for _, row in df.iterrows():
            result.append({
                "code":    str(row.get("code", "")).zfill(6),
                "name":    str(row.get("name", "")),
                "lianban": int(row.get("lianban", 1)) if pd.notna(row.get("lianban")) else 1,
                "zt_time": str(row.get("zt_time", "")),
                "amount":  _safe_float(row.get("amount")),
                "cap":     _safe_float(row.get("cap")),
                "sector":  str(row.get("sector", "")),
            })
        # 按连板数降序、成交金额降序
        result.sort(key=lambda x: (-x["lianban"], -(x["amount"] or 0)))
        return result
    except Exception as e:
        print(f"⚠️  涨停板池获取失败: {e}", file=sys.stderr)
        return []


# ──────────────────────────────────────────────
# 3. 首板候选（今日首次涨停）
# ──────────────────────────────────────────────

def get_first_zt(zt_pool: List[Dict]) -> List[Dict]:
    """
    从涨停板池中筛选首板：连板数 = 1 的个股
    龙头战法选首板关注点：
    - 早封板（涨停时间早）
    - 市值小（< 100亿）
    - 成交金额大（爆量）
    """
    first_list = [s for s in zt_pool if s.get("lianban", 1) == 1]

    # 评分：涨停时间越早得分越高；市值越小得分越高；成交越大得分越高
    def score(s: Dict) -> float:
        zt_time_str = s.get("zt_time", "15:00")
        try:
            h, m = map(int, str(zt_time_str).replace("：", ":").split(":")[:2])
            time_score = (15 * 60 - h * 60 - m) / (15 * 60 - 9 * 60 - 30)
        except Exception:
            time_score = 0

        cap = s.get("cap") or 999_0000_0000
        cap_score = 1 - min(cap / 200_0000_0000, 1)  # 200亿以下得分

        amount = s.get("amount") or 0
        amount_score = min(amount / 5_0000_0000, 1)  # 5亿成交为满分

        return time_score * 0.4 + cap_score * 0.3 + amount_score * 0.3

    first_list.sort(key=score, reverse=True)
    return first_list


# ──────────────────────────────────────────────
# 4. 连板龙头候选（连板数最高的前N只）
# ──────────────────────────────────────────────

def get_lianban_leaders(zt_pool: List[Dict], top_n: int = 5) -> List[Dict]:
    """
    筛选连板数最高的个股（市场高度股）
    龙头战法核心：做最高高度的个股
    """
    lianban = [s for s in zt_pool if s.get("lianban", 1) >= 2]
    lianban.sort(key=lambda x: (-x["lianban"], -(x["amount"] or 0)))
    return lianban[:top_n]


# ──────────────────────────────────────────────
# 5. 首阴买点（连板龙头首次出现阴线且缩量）
# ──────────────────────────────────────────────

def get_first_yin_candidates(lianban_codes: List[str]) -> List[Dict]:
    """
    扫描连板龙头中出现"首阴"的个股：
    - 近期曾经连板（连板数 ≥ 3）
    - 今日为非涨停日且收阴线
    - 成交量较前5日均量缩量 > 30%
    - 当日最低不破前一个涨停板的收盘价

    注意：此函数需要历史日线数据，对每只股票单独查询，速度较慢
    """
    if not HAS_AKSHARE:
        return []

    result = []
    today = datetime.now().strftime("%Y%m%d")
    month_start = datetime.now().replace(day=1).strftime("%Y%m%d")

    # 获取当日全市场行情
    try:
        df_spot = ak.stock_zh_a_spot_em()
        df_spot["代码"] = df_spot["代码"].astype(str).str.zfill(6)
    except Exception:
        return []

    for code in lianban_codes:
        try:
            # 判断今日是否为阴线（涨跌幅 < 0）且未涨停
            spot_row = df_spot[df_spot["代码"] == code]
            if spot_row.empty:
                continue
            change_pct = _safe_float(spot_row.iloc[0].get("涨跌幅"))
            if change_pct is None or change_pct >= 0:
                continue  # 今日非阴线，跳过

            # 获取历史日线
            market = "sh" if code.startswith("6") else "sz"
            df_hist = ak.stock_zh_a_daily(symbol=f"{market}{code}", adjust="")
            if df_hist is None or len(df_hist) < 10:
                continue

            df_hist = df_hist.sort_values("date")

            # 找近10日连板数
            recent = df_hist.tail(15)
            zt_days = 0
            prev_close = None
            last_zt_close = None
            for _, row in recent.iterrows():
                close = _safe_float(row["close"])
                if prev_close and close:
                    pct = (close - prev_close) / prev_close * 100
                    if pct >= 9.5:
                        zt_days += 1
                        last_zt_close = close
                    else:
                        if zt_days < 3:
                            zt_days = 0  # 不够3连板，重置
                prev_close = close

            if zt_days < 3 or last_zt_close is None:
                continue  # 连板不够3板，不是我们要找的首阴

            # 今日量是否缩量
            today_vol = _safe_float(df_hist.iloc[-1].get("volume")) if len(df_hist) > 0 else None
            avg_vol_5 = df_hist["volume"].tail(6).iloc[:-1].astype(float).mean() if len(df_hist) >= 6 else None

            is_shrink = (today_vol and avg_vol_5 and today_vol < avg_vol_5 * 0.7)

            # 今日最低不破上一涨停收盘价
            today_low = _safe_float(df_hist.iloc[-1].get("low"))
            not_break = (today_low and last_zt_close and today_low >= last_zt_close * 0.97)

            result.append({
                "code":          code,
                "name":          str(spot_row.iloc[0].get("名称", "")),
                "change_pct":    change_pct,
                "lianban_count": zt_days,
                "last_zt_close": last_zt_close,
                "today_low":     today_low,
                "is_shrink_vol": is_shrink,
                "not_break_low": not_break,
                "is_ideal":      bool(is_shrink and not_break),  # 理想首阴买点
            })
        except Exception:
            continue

    # 理想首阴优先
    result.sort(key=lambda x: (-int(x["is_ideal"]), -x["lianban_count"]))
    return result


# ──────────────────────────────────────────────
# 6. 板块热度（今日主线判断）
# ──────────────────────────────────────────────

def get_sector_heat(zt_pool: List[Dict]) -> List[Dict]:
    """
    统计今日涨停板中各板块出现次数，判断主线板块
    """
    sector_count: Dict[str, int] = {}
    sector_amount: Dict[str, float] = {}

    for s in zt_pool:
        sector = s.get("sector", "未知")
        if not sector:
            sector = "未知"
        sector_count[sector] = sector_count.get(sector, 0) + 1
        sector_amount[sector] = sector_amount.get(sector, 0) + (s.get("amount") or 0)

    result = [
        {
            "sector": k,
            "zt_count": v,
            "total_amount": sector_amount[k],
        }
        for k, v in sector_count.items()
    ]
    result.sort(key=lambda x: (-x["zt_count"], -x["total_amount"]))
    return result[:10]  # 返回前10热门板块


# ──────────────────────────────────────────────
# 输出格式化
# ──────────────────────────────────────────────

def fmt_amount(amount: Optional[float]) -> str:
    if amount is None:
        return "N/A"
    if amount >= 1e8:
        return f"{amount/1e8:.1f}亿"
    return f"{amount/1e4:.0f}万"


def fmt_cap(cap: Optional[float]) -> str:
    if cap is None:
        return "N/A"
    return f"{cap/1e8:.1f}亿"


def print_emotion(e: Dict):
    print("\n📡 当日市场情绪分析")
    print("=" * 60)
    if "error" in e:
        print(f"⚠️  {e['error']}")
        return
    stage = e.get("emotion_stage", "N/A")
    stage_emoji = {"高潮": "🔥", "冰点": "🧊", "极端冰点": "❄️", "正常": "📊"}.get(stage, "📊")
    src = e.get("data_source", "")
    src_note = f"（数据来源：{src}）" if src else ""
    print(f"  情绪阶段：{stage_emoji} {stage}  {src_note}")
    rc = e.get("rise_count")
    fc = e.get("fall_count")
    if rc is not None:
        print(f"  上涨家数：{rc} 家   下跌家数：{fc} 家")
    else:
        print(f"  上涨/下跌家数：（收盘后不可用，以涨停跌停数代理）")
    print(f"  涨停家数：{e.get('zt_count', 'N/A')} 家     跌停家数：{e.get('dt_count', 'N/A')} 家")
    print(f"\n  💡 操作建议：{e.get('tip', '')}")


def print_zt_pool(pool: List[Dict], top_n: int = 20):
    print(f"\n📋 今日涨停板池（前 {min(top_n, len(pool))} 只）")
    print("=" * 60)
    if not pool:
        print("  暂无数据（可能为非交易日）")
        return
    for s in pool[:top_n]:
        lb = f"【{s['lianban']}板】" if s.get("lianban", 1) > 1 else "【首板】"
        zt = s.get("zt_time", "")
        amt = fmt_amount(s.get("amount"))
        cap = fmt_cap(s.get("cap"))
        sector = s.get("sector", "")
        print(f"  {lb} [{s['code']}]{s['name']}  封板:{zt}  成交:{amt}  市值:{cap}  {sector}")


def print_first_zt(first_list: List[Dict]):
    print(f"\n🌟 首板候选（龙头战法优选，按评分排序）")
    print("=" * 60)
    if not first_list:
        print("  今日暂无首板")
        return
    for i, s in enumerate(first_list[:10], 1):
        zt = s.get("zt_time", "")
        amt = fmt_amount(s.get("amount"))
        cap = fmt_cap(s.get("cap"))
        sector = s.get("sector", "")
        print(f"  #{i} [{s['code']}]{s['name']}  封板:{zt}  成交:{amt}  市值:{cap}  {sector}")


def print_lianban(lb_list: List[Dict]):
    print(f"\n🚀 连板龙头候选（市场高度股）")
    print("=" * 60)
    if not lb_list:
        print("  今日暂无连板股")
        return
    for s in lb_list:
        amt = fmt_amount(s.get("amount"))
        cap = fmt_cap(s.get("cap"))
        sector = s.get("sector", "")
        print(f"  【{s['lianban']}板】[{s['code']}]{s['name']}  成交:{amt}  市值:{cap}  {sector}")


def print_sector_heat(sectors: List[Dict]):
    print(f"\n🔥 板块热度（主线判断）")
    print("=" * 60)
    if not sectors:
        print("  暂无数据")
        return
    for s in sectors:
        bar = "█" * s["zt_count"]
        print(f"  {s['sector']:<12} {bar} {s['zt_count']}家涨停  成交{fmt_amount(s['total_amount'])}")


def print_first_yin(fy_list: List[Dict]):
    print(f"\n📉 首阴买点候选（龙头战法经典低吸位）")
    print("=" * 60)
    if not fy_list:
        print("  今日暂无符合条件的首阴股票")
        return
    for s in fy_list:
        ideal = "✅ 理想首阴" if s.get("is_ideal") else "⚠️  参考首阴"
        print(f"  {ideal}  [{s['code']}]{s['name']}")
        print(f"      今日涨跌：{s['change_pct']:+.2f}%  连板：{s['lianban_count']}板")
        shrink = "缩量✅" if s.get("is_shrink_vol") else "未缩量⚠️"
        low_ok = "不破前低✅" if s.get("not_break_low") else "破前低⚠️"
        print(f"      {shrink}  {low_ok}  上一涨停收盘价：{s.get('last_zt_close', 'N/A')}")


# ──────────────────────────────────────────────
# 主函数
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NGA 龙头战法量化扫股工具")
    parser.add_argument("--mode", choices=["emotion", "zt", "firstzt", "lianban", "firstyin", "sector"], help="单项扫描模式")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args()

    if not HAS_AKSHARE:
        print("❌ akshare 未安装，请运行：pip install akshare pandas", file=sys.stderr)
        sys.exit(1)

    show_all = args.mode is None
    output: Dict[str, Any] = {}

    # ── Step 1: 先获取涨停板池（情绪分析非交易时段依赖它）
    zt_pool: List[Dict] = []
    need_zt = show_all or args.mode in ("zt", "firstzt", "lianban", "firstyin", "sector", "emotion")
    if need_zt:
        print("⏳ 正在获取涨停板池...", file=sys.stderr)
        zt_pool = get_zt_pool()
        output["zt_pool"] = zt_pool
        if not args.json and (show_all or args.mode == "zt"):
            print_zt_pool(zt_pool)

    # ── Step 2: 情绪（非交易时段传入已有的 zt_pool 避免重复请求）
    if show_all or args.mode == "emotion":
        emotion = get_emotion_data(zt_pool=zt_pool if zt_pool else None)
        output["emotion"] = emotion
        if not args.json:
            print_emotion(emotion)

    # ── Step 3: 首板候选
    if show_all or args.mode == "firstzt":
        first_zt = get_first_zt(zt_pool)
        output["first_zt"] = first_zt
        if not args.json:
            print_first_zt(first_zt)

    # ── Step 4: 连板龙头
    if show_all or args.mode == "lianban":
        lianban = get_lianban_leaders(zt_pool)
        output["lianban_leaders"] = lianban
        if not args.json:
            print_lianban(lianban)

    # ── Step 5: 首阴买点（需要逐只股票查历史，较慢）
    if show_all or args.mode == "firstyin":
        # 只对连板数>=3的股票扫首阴
        lianban_codes = [s["code"] for s in zt_pool if s.get("lianban", 1) >= 3]
        # 也要检查非涨停但曾经连板的：从昨日涨停池补充
        print("⏳ 正在扫描首阴买点（速度较慢）...", file=sys.stderr)
        first_yin = get_first_yin_candidates(lianban_codes)
        output["first_yin"] = first_yin
        if not args.json:
            print_first_yin(first_yin)

    # ── Step 6: 板块热度
    if show_all or args.mode == "sector":
        sectors = get_sector_heat(zt_pool)
        output["sector_heat"] = sectors
        if not args.json:
            print_sector_heat(sectors)

    # 输出摘要（全量模式）
    if show_all and not args.json and zt_pool:
        emotion = output.get("emotion", {})
        first_zt_list = get_first_zt(zt_pool)
        lianban_list  = get_lianban_leaders(zt_pool)
        sectors_list  = get_sector_heat(zt_pool)

        print("\n" + "=" * 60)
        print("📊 龙头战法扫股摘要")
        print("=" * 60)
        stage = emotion.get("emotion_stage", "N/A")
        print(f"  情绪阶段：{stage}   涨停：{emotion.get('zt_count','N/A')}家   跌停：{emotion.get('dt_count','N/A')}家")
        if sectors_list:
            print(f"  今日主线：{sectors_list[0]['sector']}（{sectors_list[0]['zt_count']}家涨停）")
        if lianban_list:
            top = lianban_list[0]
            print(f"  市场高度：{top['lianban']}板 [{top['code']}]{top['name']}")
        if first_zt_list:
            print(f"  首板前3：{'  '.join([s['name'] for s in first_zt_list[:3]])}")
        print("\n  💡 龙头战法操作建议：")
        print(f"     {emotion.get('tip', '按盘面机动')}")
        print()

    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
