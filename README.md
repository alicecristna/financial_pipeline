# Financial Data Pipeline v1.0

金融数据处理管道：从数据获取、清洗、存储到分析的一站式工具包。

## 模块

| 模块 | 文件 | 功能 |
|---|---|---|
| **DataFetcher** | `data_fetcher.py` | 统一数据下载接口（美股/商品/宏观 → yfinance，A股 → baostock），支持重试和增量更新 |
| **AsyncFetcher** | `async_fetcher.py` | 异步并发下载多只股票，财经新闻 RSS 爬虫 |
| **DatabaseManager** | `database.py` | SQLite 存储（WAL 模式），批量事务写入，参数化查询防注入 |
| **DataPipeline** | `pipeline.py` | 端到端管道协调：下载 → 格式转换 → 写入数据库 → 日志，幂等设计 |
| **PDFCleaner** | `pdf_cleaner.py` | PDF 年报文本和表格提取 |
| **Utils** | `utils.py` | 日志配置，数据校验 |

## 快速开始

```python
from data_fetcher import DataFetcher
from database import DatabaseManager
from pipeline import DataPipeline

# 下载美股数据
fetcher = DataFetcher()
df = fetcher.fetch_yfinance(["AAPL", "GOOGL", "MSFT"], period="6mo")
prices = fetcher.get_prices(df)

# 一键管道：下载 → 写入数据库
pipeline = DataPipeline(db_path="finance.db")
pipeline.update_daily_prices(["AAPL", "GOOGL", "MSFT"], years_back=2)

# 从数据库读取价格矩阵
matrix = pipeline.get_price_matrix()
print(pipeline.db.get_summary())

pipeline.close()
```

### A 股数据

```python
prices = fetcher.fetch_a_shares(
    ["sh.600519", "sz.000858"],   # 贵州茅台, 五粮液
    start="2024-01-01",
    adjust="2"                    # 前复权
)
```

### 异步并发下载

```python
from async_fetcher import AsyncFetcher
fetcher = AsyncFetcher()
prices = fetcher.download_sync(
    ["AAPL", "GOOGL", "MSFT", "NVDA", "AMZN"],
    start="2025-01-01",
    end="2026-05-08"
)
```

## 安装依赖

```bash
pip install yfinance pandas baostock aiohttp beautifulsoup4 pdfplumber lxml
```
