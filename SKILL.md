---
name: stock-trading
description: This skill should be used when the user asks about stock/ETF analysis, investment strategy, technical analysis, fundamental analysis, risk management, market sentiment, portfolio review, trading decisions, NGA dragon method (龙头战法), or ETF trading. Triggers on keywords like "选股", "炒股", "ETF", "基金", "指数基金", "定投", "行业ETF", "宽基", "沪深300", "中证500", "创业板", "纳指", "黄金ETF", "溢价", "折价", "轮动", "帮我看看股票", "分析持仓", "行业龙头", "我的自选股", "技术分析", "基本面", "K线", "MACD", "RSI", "估值", "仓位", "止损", "A股", "美股", "港股", "龙头战法", "涨停", "连板", "首板", "情绪周期", "冰点", "高潮", "板块联动", "扫股", "推荐股票", "stock analysis", "stock picking", "ETF scan", "index fund", "portfolio".
version: 1.3.0
---

# 炒股辅助 Skill

专为个人投资者设计的综合投资辅助 skill，支持：
- **A 股行业龙头自动扫描**与**用户自选股实时行情**
- **NGA 龙头战法量化扫股**：情绪判断、涨停板池、首板候选、连板龙头、首阴买点
- **ETF 交易全套工具**：行情排行、动量筛选、超跌扫描、持仓管理、估值参考

> ⚠️ **免责声明**：本 skill 仅提供信息参考，不构成任何投资建议。股市有风险，入市需谨慎。

---

## 数据获取（核心能力）

### 方式一：自动获取 A 股行业龙头

在回答用户关于"行业龙头"、"哪些股票值得看"、"全市场扫描"等问题时，**优先调用脚本获取数据，而不是等用户输入代码**：

```bash
# 获取全部行业龙头（含实时行情）
python scripts/fetch_stocks.py --leaders

# 获取指定行业龙头
python scripts/fetch_stocks.py --industry 白酒

# JSON 格式输出（供程序化处理）
python scripts/fetch_stocks.py --leaders --json
```

覆盖行业（18 个）：
白酒 · 银行 · 新能源/动力电池 · 半导体/芯片 · 医药/创新药 · 医疗器械 ·
消费电子 · 互联网/软件 · 军工/国防 · 煤炭/能源 · 钢铁/有色金属 ·
房地产 · 保险/券商 · 零售/消费 · AI/人工智能 · 汽车整车 · 化工 · 农业/养殖

### 方式二：读取用户自选股

用户的个人持仓记录在 `my_portfolio.json`，运行脚本即可获取含**实时行情的持仓列表**：

```bash
# 获取自选股实时行情（含盈亏计算）
python scripts/fetch_stocks.py --portfolio
```

用户如何编辑自选股 → 直接修改 `my_portfolio.json`，格式如下：

```json
{
  "stocks": [
    {
      "code": "600519",
      "name": "贵州茅台",
      "market": "SH",
      "cost_price": 1680.00,
      "shares": 10,
      "note": "核心持仓，长线"
    }
  ]
}
```

### 数据依赖

- **实时行情**：依赖 `akshare`（`pip install akshare`）
- **无 akshare 降级**：脚本仍可运行，但只输出静态列表（无实时价格/PE/PB）
- 详见 `references/data-sources.md`

---

## 主要使用场景

### 场景 A：用户让你扫描行业龙头

**触发语**：「帮我看看各行业龙头」「A 股现在有哪些好股票」「推荐几个值得关注的股票」

**执行步骤**：

1. 调用 `python scripts/fetch_stocks.py --leaders` 获取行业龙头列表
2. 解析输出，重点关注：
   - 涨跌幅较大的个股（异动提示）
   - PE/PB 处于历史低位的个股（估值洼地）
3. 按以下结构输出分析摘要：

```
📊 今日行业龙头行情摘要（共 XX 行业）

【涨幅前5】
  ...

【跌幅前5】
  ...

【估值偏低（PE < 15 且 PB < 2）】
  ...

是否需要深入分析某只个股或某个行业？
```

---

### 场景 B：用户让你分析自选股

**触发语**：「看看我的持仓」「分析一下我的股票」「我的自选股今天怎么样」

**执行步骤**：

1. 调用 `python scripts/fetch_stocks.py --portfolio`
2. 若 `my_portfolio.json` 为空或不存在，提示用户：
   > "请编辑 `my_portfolio.json` 文件，添加你持有的股票代码和成本价"
3. 计算每只股票的**浮动盈亏**（如果有 cost_price）：
   ```
   浮盈 = (当前价 - 成本价) / 成本价 × 100%
   浮盈金额 = (当前价 - 成本价) × 持有股数
   ```
4. 按以下结构输出持仓报告：

```
📁 我的持仓报告

股票         现价    涨跌幅    成本价    浮动盈亏
贵州茅台     1720    +1.2%    1680     +2.4%（+4000元）
宁德时代      190    -0.8%     185     +2.7%（+500元）
招商银行      35.2   +0.5%    32.5    +8.3%（+1350元）

总持仓估值：XXX元
总浮动盈亏：+XX元（+XX%）

[技术面快速预警]
- 宁德时代：临近60日均线支撑，注意量能
- 招商银行：MACD 金叉形成，短期偏多
```

---

### 场景 C：用户让你分析某只个股

**触发语**：「帮我分析 茅台」「600519 现在怎么样」「宁德时代值得买吗」

**执行步骤**：

1. 调用 `python scripts/fetch_stocks.py --json` 获取全量数据，在结果中定位该股票
2. 如果在龙头列表或自选股中找到 → 直接使用数据
3. 如果未找到 → 提示用户确认代码，或通过 akshare 单独查询：
   ```python
   import akshare as ak
   df = ak.stock_individual_info_em(symbol="600519")
   ```
4. 按「个股完整分析报告」模板输出（详见 `examples/stock-analysis-example.md`）

---

### 场景 D：用户问选股策略

引导用户明确：**投资风格 / 持仓周期 / 风险偏好**，然后从行业龙头列表中按条件筛选，输出匹配的候选股票。

---

### 场景 E：NGA 龙头战法 — 情绪周期判断

**触发语**：「帮我用龙头战法扫股」「今天有哪些首板」「现在市场什么阶段」「该重仓还是轻仓」「连板龙头是谁」「有没有首阴买点」

详细判断框架 → `references/scenario-f-emotion-cycle.md` | 扫股步骤 → `references/scenario-e-dragon-scan.md`

```bash
python3 scripts/scan_dragon.py                 # 全量扫描
python3 scripts/scan_dragon.py --mode emotion  # 仅情绪周期
python3 scripts/scan_dragon.py --mode firstzt  # 仅首板
python3 scripts/scan_dragon.py --mode lianban  # 仅连板
python3 scripts/scan_dragon.py --mode firstyin # 仅首阴买点
```

---

### 场景 F：ETF 分析（扫描 / 选择 / 持仓）

**触发语**：「帮我扫ETF」「今天哪些ETF涨了」「ETF怎么选」「沪深300哪个ETF好」「分析我的ETF持仓」「ETF浮盈多少」

扫描步骤 → `references/scenario-g-etf-scan.md` | 选择标准 → `references/scenario-h-etf-selection.md`

```bash
python3 scripts/scan_etf.py --mode core       # 核心ETF监控
python3 scripts/scan_etf.py --mode rank       # 今日涨跌榜
python3 scripts/scan_etf.py --mode momentum   # 近20日动量
python3 scripts/scan_etf.py --mode oversold   # 超跌筛选
python3 scripts/scan_etf.py --mode valuation  # 估值参考
python3 scripts/scan_etf.py --mode portfolio  # 我的持仓分析
```

回答时结合 `references/etf-trading.md`：
- **扫描/推荐**：定投倍数(2.1节)、超跌低吸(2.3节)、行业轮动(2.2节)、PE分位(第四章)
- **选品对比**：规模 > 跟踪误差 > 费率 > 折溢价接近0（第三章）
- **持仓建议**：浮盈>20%参考止盈策略(第六章)、浮亏>15%参考补仓条件(第五章)、集中度风险(第七章)

编辑持仓 → `my_etf_portfolio.json`

---


## 分析报告结构（个股）

按以下顺序输出：

1. **基本信息**：代码 · 名称 · 行业 · 当前价 · PE · PB · 市值
2. **技术面**：趋势方向 · 均线排列 · MACD · RSI · 量价 · 支撑/压力位
3. **基本面**：营收增速 · 毛利率 · ROE · 负债率 · 估值历史分位
4. **风险提示**：核心风险 1-3 条 · 止损参考位
5. **综合结论**：📈 偏多 / 📊 中性 / 📉 偏空 + 操作参考

---

## 快速响应模板

```
【股票代码】xxx（名称）
现价：xx 元  涨跌：±x.x%  PE：xx  PB：x.x
技术面：[一句话概括]
基本面：[一句话概括]
操作建议：[买入/观望/规避]  止损参考：[价位]
风险提示：[1-2个核心风险]
```

---

## 支持文件

**个股相关**
- `scripts/fetch_stocks.py` — 行业龙头 + 自选股行情脚本（akshare 驱动）
- `scripts/scan_dragon.py` — **龙头战法量化扫股脚本**（涨停板池/首板/连板/首阴）
- `my_portfolio.json` — 用户自选股配置
- `references/nga-dragon-method.md` — **NGA 龙头战法完整手册**

**ETF 相关**
- `scripts/scan_etf.py` — **ETF 行情扫描脚本**（涨跌榜/动量/超跌/持仓/估值）
- `my_etf_portfolio.json` — 用户 ETF 持仓配置
- `references/etf-trading.md` — **ETF 交易完整手册**（策略/选品/择时/仓位）
- `references/scenario-g-etf-scan.md` — 场景G：ETF行情扫描详细步骤与分析模板
- `references/scenario-h-etf-selection.md` — 场景H：ETF选择与对比标准

**示例**
- `examples/stock-analysis-example.md` — 完整分析示例

---

## 注意事项

1. **优先调用脚本获取数据**，不要直接让用户输入股票代码
2. 分析时区分 A股 / 美股 / 港股，规则和估值体系有差异
3. 技术分析与基本面分析要相互印证，避免单一依赖
4. 任何分析结论都要附带风险提示
5. 不对任何股票做出"必涨"或"必跌"的确定性判断
6. 若 akshare 返回数据异常，说明原因并给出静态列表分析
