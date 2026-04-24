# 场景 G：ETF 行情扫描与推荐

**触发语**：「帮我扫一下ETF」「今天哪些ETF涨了」「最强势的ETF是哪个」「有没有超跌的ETF」「给我推荐几个ETF」

---

## 执行步骤

```bash
# 今日涨跌幅排行（最常用）
python3 scripts/scan_etf.py --mode rank

# 核心 ETF 行情监控（宽基+主要行业）
python3 scripts/scan_etf.py --mode core

# 近20日动量筛选（趋势追踪）
python3 scripts/scan_etf.py --mode momentum

# 超跌筛选（从1年高点回撤≥25%，低吸候选）
python3 scripts/scan_etf.py --mode oversold

# 宽基指数估值参考（择时辅助）
python3 scripts/scan_etf.py --mode valuation

# 全量扫描（含上述全部）
python3 scripts/scan_etf.py
```

---

## 输出解读方式

```
📈 今日 ETF 涨幅榜 TOP 15
  #1 [512480]半导体ETF    +3.20%   成交:32.4亿
  #2 [515070]人工智能ETF  +2.80%   成交:28.6亿

📊 核心 ETF 行情监控
  ── 宽基 ──
  📉 [510300]沪深300ETF   4.801  -0.21%   成交:48亿
  📈 [510880]红利ETF      3.293  +0.73%   成交:4亿   ← 防御逆势强

  ── 科技 ──
  📉 [512480]半导体ETF    1.648  -1.73%
```

---

## 推荐分析模板（输出给用户时包含）

```
【今日市场概况】
  涨幅前3：XXX ETF / XXX ETF / XXX ETF
  跌幅前3：XXX ETF / XXX ETF / XXX ETF
  防御板块（红利/黄金/债券）：[逆势涨 / 同跌]

【主线方向判断】
  今日成交最活跃板块：[XX ETF，成交XX亿]
  近20日动量最强：[XXX ETF，+XX%]

【超跌机会】（如有）
  [代码]名称  回撤:XX%  现价:X.XX  1年高点:X.XX
  操作提示：基本面未恶化则可关注分批低吸，参考 references/etf-trading.md 超跌策略

【宽基估值参考】
  沪深300 PE：XX（[低估/正常/高估]）→ [定投加码/正常/止盈区间]
```

---

## 核心 ETF 预设监控列表

| 类型 | 代码 | 名称 |
|------|------|------|
| 宽基 | 510300 | 沪深300ETF |
| 宽基 | 510500 | 中证500ETF |
| 宽基 | 159915 | 创业板ETF |
| 宽基 | 510880 | 红利ETF |
| 宽基 | 159338 | 中证A500ETF |
| 科技 | 512480 | 半导体ETF |
| 科技 | 515070 | 人工智能ETF |
| 新能源 | 516160 | 新能源ETF |
| 金融 | 512800 | 银行ETF |
| 金融 | 512880 | 券商ETF |
| 跨境 | 513100 | 纳指ETF |
| 商品 | 518880 | 黄金ETF |

完整选品标准 → `references/etf-trading.md` 第三章
