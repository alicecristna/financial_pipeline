"""
DatabaseManager — SQLite 行情数据存取
"""

"""主流的防止sql注入的参数化方法有两种，一种是用问号占位符号，另一种是使用字典进行命名占位 """
import sqlite3
import pandas as pd
from pathlib import Path
import os

work_dir = Path.home()/"Desktop"/'coding'/'data_analysis'/'financial_pipeline'
os.chdir(work_dir)
from utils import setup_logger

logger = setup_logger(__name__)

class DatabaseManager:
    """SQLite 数据库：建表、索引、写入、查询、聚合"""

    def __init__(self, db_path: str = "financial_data.db"):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -64000;
        PRAGMA temp_store = MEMORY;
    """)
        self.conn.commit()

        logger.info(f"数据库连接: {self.db_path}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ========== 建表 + 索引 ==========
    def create_tables(self):
        """建表并创建必要的索引"""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                ticker TEXT NOT NULL,
                date   TEXT NOT NULL,
                open   REAL,
                high   REAL,
                low    REAL,
                close  REAL,
                volume INTEGER,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS tickers (
                ticker   TEXT PRIMARY KEY,
                name     TEXT,
                sector   TEXT,
                exchange TEXT
            );

            CREATE TABLE IF NOT EXISTS update_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ticker      TEXT,
                rows_added  INTEGER
            );

            -- 只有 date 需要单独索引
            -- ticker 的查询已被复合主键的左前缀覆盖
            CREATE INDEX IF NOT EXISTS idx_date ON daily_prices(date);
        """)
        self.conn.commit()
        logger.info("数据表和索引创建完成")

    # ========== 写入（事务包裹） ==========
    def insert_prices(self, df: pd.DataFrame):
        """
        批量写入行情数据（事务包裹，主键冲突时跳过）

        原理：SQLite 默认每条语句一个事务（打开→写盘→关闭→写日志），
              1000 条就是 1000 次。with self.conn 把 1000 条包成一个事务，
              只做一次打开/关闭，速度提升 100 倍以上。
        """
        cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        available = [c for c in cols if c in df.columns]
        data = df[available].copy()

        if "date" in data.columns:
            data["date"] = data["date"].astype(str)

        with self.conn:  # 自动 BEGIN + COMMIT
            data.to_sql(
                "daily_prices",
                self.conn,
                if_exists="append",
                index=False,
            )

        logger.info(f"写入 {len(data)} 条记录")

    # ========== 查询 ==========
    def get_prices(
        self,
        ticker: str = None,
        start: str = None,
        end: str = None,
    ) -> pd.DataFrame:
        """按条件查询行情"""
        clauses = []
        params = []

        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)

        where = " AND ".join(clauses) if clauses else "1=1"
        return pd.read_sql_query(
            f"SELECT * FROM daily_prices WHERE {where}",
            self.conn,
            params=params,
        )

    # ========== 聚合 ==========
    def get_summary(self) -> pd.DataFrame:
        """
        在数据库端计算每只股票的基本统计

        为什么在 SQL 端算？数据库引擎用 C 写，聚合运算极快；
        拉回 Python 再 groupby 等于把所有数据搬过来再算，多了一道搬运。
        """
        return pd.read_sql_query("""
            SELECT
                ticker,
                COUNT(*)  AS days,
                ROUND(AVG(close), 2) AS avg_close,
                MAX(close) AS max_close,
                MIN(close) AS min_close,
                AVG(volume) AS avg_volume
            FROM daily_prices
            GROUP BY ticker
        """, self.conn)

    # ========== 关闭 ==========
    def close(self):
        self.conn.close()
        logger.info("数据库连接关闭")

 