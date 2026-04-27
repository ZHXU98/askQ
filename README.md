# 📈 stock-trading-skill

一个专为个人投资者设计的**炒股辅助工具集**，帮助你进行股票分析、龙头战法扫股、ETF 投资决策。

> ⚠️ **免责声明**：本项目仅提供信息参考，不构成任何投资建议。股市有风险，入市需谨慎。

---

## 功能特性

- **📊 行业龙头扫描**：18个行业龙头股实时行情 + PE/PB 估值
- **🐲 龙头战法扫股**：情绪周期判断、涨停板池、首板/连板/首阴候选
- **📈 ETF 行情监控**：核心ETF实时行情、涨跌榜、动量/超跌筛选
- **💼 持仓分析**：自动计算浮盈浮亏、净值历史分位参考

---

## 目录结构

```
stock-trading-skill/
├── SKILL.md                            # Skill 主入口
├── README.md                           # 本文件
├── my_portfolio.json                   # 个股持仓配置（需自行配置）
├── my_etf_portfolio.json               # ETF 持仓配置（需自行配置）
├── scripts/
│   ├── fetch_stocks.py                 # 行业龙头 + 自选股行情
│   ├── scan_dragon.py                  # 龙头战法量化扫股
│   └── scan_etf.py                     # ETF 行情扫描
├── references/
│   ├── nga-dragon-method.md            # NGA 龙头战法手册
│   ├── etf-trading.md                  # ETF 交易手册
│   ├── scenario-e-dragon-scan.md       # 场景E：龙头扫股步骤
│   ├── scenario-f-emotion-cycle.md      # 场景E：情绪周期判断
│   ├── scenario-g-etf-scan.md          # 场景F：ETF扫描步骤
│   └── scenario-h-etf-selection.md     # 场景F：ETF选择标准
└── examples/
    └── stock-analysis-example.md       # 个股分析示例
```

---

## 安装使用

克隆项目到本地：

```bash
git clone https://github.com/ZHXU98/askQ.git
cd askQ
```

安装依赖：

```bash
pip install akshare pandas
```

---

## 快速上手

### 个股分析

```
帮我分析一下 贵州茅台（600519），是否适合买入？
```

### 龙头战法扫股

```
帮我用龙头战法扫股，今天有哪些首板？
现在市场情绪什么阶段？
```

### ETF 分析

```
帮我扫一下ETF，今天哪些涨了？
分析我的ETF持仓，浮盈多少？
沪深300哪个ETF好？
```

---

## 支持的场景

| 场景 | 触发语 | 命令 |
|------|--------|------|
| A 行业龙头 | 「推荐股票」「行业龙头」 | `python scripts/fetch_stocks.py --leaders` |
| B 自选股 | 「看看我的持仓」 | `python scripts/fetch_stocks.py --portfolio` |
| C 个股分析 | 「分析茅台」 | — |
| D 选股策略 | 「给我选股思路」 | — |
| E 龙头战法 | 「今天首板」「市场情绪」 | `python scripts/scan_dragon.py` |
| F ETF 扫描 | 「帮我扫ETF」「今天ETF涨跌」 | `python scripts/scan_etf.py --mode rank` |
| G ETF 持仓 | 「我的ETF浮盈」「分析我的ETF持仓」 | `python scripts/scan_etf.py --mode portfolio` |
| H ETF 资金流向 | 「ETF资金流向」「哪些ETF资金流入」 | `python scripts/scan_etf.py --mode market_flow` |

---

## 参考资料

- [NGA 龙头战法手册](references/nga-dragon-method.md)
- [ETF 交易手册](references/etf-trading.md)
- [个股分析示例](examples/stock-analysis-example.md)

---

## License

MIT License
