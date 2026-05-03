"""
processor.py — 金融数据读取 & 预处理
"""
import os
import pandas as pd
from utils import setup_logger, optimize_dtypes

logger = setup_logger(__name__)


class DataProcessor:
    def __init__(self, data_dir="./data/"):
        self.data_dir = data_dir
        self.raw_data = None
        self.prices = None

    # ---------- 任务 10：关键位置日志埋点 ----------
    def load_raw(self, optimize: bool = True):
        """读取 data_dir 下所有 CSV 文件，合并为一个大 DataFrame"""
        df_list = []

        for file in os.listdir(self.data_dir):
            if not file.endswith(".csv"):
                continue

            file_path = os.path.join(self.data_dir, file)
            logger.debug("正在读取文件: %s", file_path)

            # ----- 尝试多种编码读取 -----
            for encoding in ["utf-8", "gbk", "gb2312", "latin-1"]:
                try:
                    df = pd.read_csv(file_path, skiprows=[1], encoding=encoding)
                    break
                except UnicodeDecodeError:
                    logger.warning("编码 %s 失败，尝试下一种编码: %s", encoding, file)
                except Exception as e:
                    logger.error("文件 %s 完全无法读取: %s", file_path, e)
                    raise

            ticker = file.replace(".csv", "")
            df["ticker"] = ticker
            df["date"] = pd.to_datetime(df["Date"])
            df_list.append(df)

        if not df_list:
            logger.warning("data_dir 下未找到任何 CSV 文件: %s", self.data_dir)
            return pd.DataFrame()

        self.raw_data = pd.concat(df_list, ignore_index=True)
        logger.info("所有文件读取完成，共 %d 个文件，%d 条记录",
                     len(df_list), len(self.raw_data))

        # 自动优化 dtype
        if optimize:
            self.raw_data = optimize_dtypes(self.raw_data)
            logger.debug("dtype 优化完成")

        return self.raw_data

    def process_prices(self, price_col="Close", fill_method="ffill"):
        """从 raw_data 中提取价格矩阵"""
        if self.raw_data is None:
            self.load_raw()

        prices = {}
        for ticker, group in self.raw_data.groupby("ticker"):
            prices[ticker] = group.set_index("date")[price_col]

        self.prices = pd.DataFrame(prices).sort_index()

        if fill_method == "ffill":
            self.prices = self.prices.ffill()

        logger.debug("价格矩阵构建完成，shape=%s", self.prices.shape)
        return self.prices

    def process_returns(self):
        """输出日收益率矩阵"""
        if self.prices is None:
            self.process_prices()
        return self.prices.pct_change().dropna()

    def get_stats(self):
        """输出基本统计量"""
        if self.prices is None:
            self.process_prices()
        returns = self.process_returns()

        stats = pd.DataFrame({
            "mean": returns.mean(),
            "var": returns.var(),
            "skew": returns.skew(),
        })
        return stats