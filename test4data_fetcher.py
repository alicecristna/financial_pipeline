from pathlib import Path
import os
import pandas as pd
import time
work_dir = Path.home() / "Desktop" / "coding" / "data_analysis" / "financial_pipeline"

os.chdir(work_dir)
print(f"工作目录: {os.getcwd()}")

from data_fetcher import DataFetcher
from factor_backtest import FactorBacktest

from utils import setup_logger , validate_price_data


logger = setup_logger(__name__)


def main():
    # ---- 1. 下载 ----
    logger.info("=" * 50)
    logger.info("第 1 步：下载美股数据")
    fetcher = DataFetcher()
    # 在 test4data_fetcher.py 中，改成逐只下载再合并
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    prices_list = []

    for t in tickers:
        raw = fetcher.fetch_yfinance([t], start="2023-01-01", end="2024-12-31")
        p = fetcher.get_prices(raw)
        if not p.empty:
            prices_list.append(p)
        time.sleep(2)  # 每只间隔 2 秒，避免限流

    prices = pd.concat(prices_list, axis=1) if prices_list else pd.DataFrame()

    ok, msg = validate_price_data(prices, name="美股")
    if not ok:
        logger.error(f"数据验证失败: {msg}")
        return

    # ---- 2. 算收益率 ----
    logger.info("=" * 50)
    logger.info("第 2 步：计算日收益率")
    returns = prices.pct_change().dropna()
    logger.info(f"收益率矩阵: {returns.shape}")

    # ---- 3. 回测 ----
    logger.info("=" * 50)
    logger.info("第 3 步：动量策略回测")
    bt = FactorBacktest(prices)
    mom = bt.momentum_factor(window=20)
    signals = bt.generate_signals(mom, long_threshold=0.02)
    bt.calculate_strategy_returns(signals)
    perf = bt.calculate_performance()

    logger.info(f"✅ 端到端测试通过")
    logger.info(f"  夏普比率: {perf['sharpe_ratio']:.4f}")
    logger.info(f"  最大回撤: {perf['max_drawdown']:.4%}")


if __name__ == "__main__":
    main()