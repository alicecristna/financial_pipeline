# Financial Data Pipeline v1.0

金融数据处理管道：从数据获取、清洗、存储到分析的一站式工具包。

## 技术亮点

- **双源数据获取**：yfinance + baostock 覆盖美股和 A 股，支持自动重试、请求限流和增量更新
- **高性能写入**：SQLite WAL 模式 + 批量事务，写入性能达 10000+ 行/秒
- **SQL 端聚合**：在数据库端完成 AVG/MAX/MIN/COUNT 等统计，减少 Python 端数据搬运
- **幂等管道**：下载 → 格式转换 → 写入 → 日志全链路幂等设计，可重复执行
- **数据质量校验**：空值检测、日期单调性校验、未来日期检查

## 模块

| 模块 | 文件 | 功能 |
|---|---|---|
| **DataFetcher** | `data_fetcher.py` | 统一数据下载接口（美股 → yfinance，A 股 → baostock），含重试机制和增量更新引擎 |
| **AsyncFetcher** | `async_fetcher.py` | 异步并发下载（asyncio + ThreadPoolExecutor），RSS 财经新闻抓取（aiohttp + BeautifulSoup） |
| **DatabaseManager** | `database.py` | SQLite 存储，复合主键 + 索引设计，WAL 模式优化，批量事务写入，参数化查询防注入 |
| **DataPipeline** | `pipeline.py` | 端到端管道编排：下载 → 格式转换（pivot/melt）→ 写入数据库 → 日志记录 |
| **PDFCleaner** | `pdf_cleaner.py` | pdfplumber 提取 PDF 年报文本和表格，正则表达式清洗 |
| **Utils** | `utils.py` | 日志配置（控制台 + 文件双输出），数据质量校验 |

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

# SQL 端聚合统计（AVG/MAX/MIN/COUNT）
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
