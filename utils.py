"""utils.py — 日志配置 & dtype 优化"""
import logging
from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)

def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """日志配置"""
    if log_file is None:
        log_file = str(BASE_DIR / "logs" / "pipeline.log")

    
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(fh)
    return logger

def validate_price_data(df: pd.DataFrame, name: str = ""):
    """返回 (是否通过, 错误信息)"""
    label = f"[{name}] " if name else ""

    if df.empty:
        return False, f"{label}数据为空"
    if df.isna().all(axis=1).any():
        bad_rows = df[df.isna().all(axis=1)].index[:3].tolist()
        return False, f"{label}全 NaN 行: {bad_rows}"
    if not df.index.is_monotonic_increasing:
        return False, f"{label}日期不单调"
    if df.index.max() > pd.Timestamp.now():
        return False, f"{label}存在未来日期: {df.index.max()}"

    return True, ""