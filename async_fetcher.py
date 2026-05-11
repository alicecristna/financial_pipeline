"""
AsyncFetcher — 异步并发数据下载

核心思路：
    yfinance 是同步库 → 用 run_in_executor 扔到线程池跑
    HTTP 请求是 I/O → 用 aiohttp 原生异步
    两种模式合并 → asyncio.gather 并发执行
"""
import asyncio
import aiohttp
import pandas as pd
from typing import List
from pathlib import Path
import os
import random
from bs4 import BeautifulSoup

work_dir = Path.home() / "Desktop" / "coding" / "data_analysis" / "financial_pipeline"
os.chdir(work_dir)

from utils import setup_logger

logger = setup_logger(__name__)


class AsyncFetcher:
    """异步数据获取：并发下载多只股票 + 并发抓取网页"""

    def __init__(self):
       return 

    async def download_stocks(self, tickers: List[str], **kwargs) -> pd.DataFrame:
        loop = asyncio.get_running_loop()

        async def fetch_one(ticker):#这里另外设置local_fetcher是为了防止线程污染
            try:
                from data_fetcher import DataFetcher
                local_fetcher = DataFetcher()  # 独立的 fetcher 实例

                df = await loop.run_in_executor(
                    None,
                    local_fetcher.fetch_yfinance,
                    [ticker],
                    kwargs.get("start"),
                    kwargs.get("end"),
                )
                prices = local_fetcher.get_prices(df)

                if not prices.empty:
                    single = prices.iloc[:, [0]]
                    single.columns = [ticker]
                    return single

                return pd.DataFrame()

            except Exception as e:
                logger.error(f"{ticker}: {e}", exc_info=True)
                return pd.DataFrame()

        tasks = [fetch_one(t) for t in tickers]
        results = await asyncio.gather(*tasks)

        combined = pd.concat([r for r in results if not r.empty], axis=1)
        logger.info(f"下载完成: {len(tickers)} 只 → {combined.shape}")
        return combined

    def download_sync(self, tickers: List[str], **kwargs) -> pd.DataFrame:
        return asyncio.run(self.download_stocks(tickers, **kwargs))



class NewsScraper:
    """
    财经新闻爬虫（学习用途，遵守 robots.txt）

    两个控制并发的手段：
      Semaphore — 限制同时发出的请求数（不超 max_concurrency）
      delay     — 控制请求间隔，避免触发反爬
    """

    def __init__(self, max_concurrency: int = 5, delay: float = 1.0):
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.delay = delay

    async def fetch(self, url: str, timeout: int = 10) -> dict:
        """
        单次受控请求：信号量限并发 + 随机延迟间隔
        """
        await asyncio.sleep(random.uniform(0.5, self.delay))

        async with self.semaphore:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
            #这段代码是模拟一个普通的，苹果系统的chrome浏览器
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, headers=headers, timeout=timeout) as resp:
                        html = await resp.text() if resp.status == 200 else ""
                        return {"url": url, "html": html, "status": resp.status}
                except asyncio.TimeoutError:
                    return {"url": url, "error": "Timeout"}
                except Exception as e:
                    return {"url": url, "error": str(e)}

    async def fetch_all(self, urls: List[str], timeout: int = 10) -> List[dict]:
        """并发抓取多个 URL"""
        tasks = [self.fetch(url, timeout) for url in urls]
        return await asyncio.gather(*tasks)

    @staticmethod
    def parse_rss(html: str) -> list:
        """
        解析 RSS 2.0 XML 格式的新闻列表

        返回: [{"title": ..., "link": ..., "description": ...}, ...]
        """
        soup = BeautifulSoup(html, "xml")
        #BeautifulSoup里面的.find 和 普通python里面的.find是不一样的，前者是截取html树的整个标签和字符给你，后者是将索引返还给你
        items = []
        for item in soup.find_all("item"):
            title = item.find("title")
            link = item.find("link")
            desc = item.find("description")
            items.append({
                "title": title.text if title else "",
                "link": link.text if link else "",
                "description": desc.text[:200] if desc else "",
            })
        return items


# ============================================
# 入口
# ============================================
async def main():
    # 1. 股票下载
    fetcher = AsyncFetcher()
    prices = await fetcher.download_stocks(
        ["AAPL", "GOOGL", "MSFT"],
        start="2025-01-01",
        end="2026-05-08",
    )
    print("=== 股票价格 ===")
    print(prices)

    # 2. 新闻抓取
    scraper = NewsScraper(max_concurrency=3, delay=1.0)

    urls = [
        "https://finance.yahoo.com/news/rssindex",
        "https://feeds.bloomberg.com/markets/news.rss",
    ]
    results = await scraper.fetch_all(urls)

    for r in results:
        if r.get("html"):
            articles = NewsScraper.parse_rss(r["html"])#把rss xml解析成文章列表
            print(f"\n{r['url']}: {len(articles)} 篇文章")
            for a in articles[:3]:
                print(f"  • {a['title']}")


if __name__ == "__main__":
    asyncio.run(main())