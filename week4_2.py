import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
import time

# 添加模块路径
sys.path.append(str(Path.home() / 'Desktop' / '代码' / 'data analysis' / '新征程'))

from processor import DataProcessor

# ============================================================
# 设置工作目录（确保文件写入到正确位置）
# ============================================================
work_dir = Path.home() / 'Desktop' / '代码' / 'data analysis' / '新征程'
os.chdir(work_dir)  # 强制切换工作目录
print(f"工作目录: {os.getcwd()}")

# 初始化
processor = DataProcessor(str(work_dir / 'data'))
processor.load_raw()

# ============================================================
# 任务 5-7 的代码
# ============================================================
raw_df = processor.raw_data

# ... 省略中间代码 ...

df_optimized = raw_df.copy()
# ... 优化代码 ...

# ============================================================
# 【任务 8】Parquet vs CSV 性能对比（使用绝对路径）
# ============================================================

print("\n=== 任务 8：Parquet vs CSV 性能对比 ===")

test_df = df_optimized

# 关键：使用绝对路径
csv_path = str(work_dir / 'test_data.csv')
parquet_path = str(work_dir / 'test_data.parquet')

print(f"CSV 路径: {csv_path}")
print(f"Parquet 路径: {parquet_path}")

# CSV 写入
start = time.time()
test_df.to_csv(csv_path, index=False)
csv_write_time = time.time() - start

# Parquet 写入
start = time.time()
test_df.to_parquet(parquet_path, index=False)
parquet_write_time = time.time() - start

# CSV 读取
start = time.time()
_ = pd.read_csv(csv_path)
csv_read_time = time.time() - start

# Parquet 读取
start = time.time()
_ = pd.read_parquet(parquet_path)
parquet_read_time = time.time() - start

# 文件大小
csv_size = os.path.getsize(csv_path) / 1024**2
parquet_size = os.path.getsize(parquet_path) / 1024**2

# 结果输出
print(f"\n{'指标':<20} {'CSV':>12} {'Parquet':>12} {'提升':>12}")
print("-" * 56)
print(f"{'写入时间 (秒)':<20} {csv_write_time:>12.4f} {parquet_write_time:>12.4f} {csv_write_time/parquet_write_time:>11.1f}x")
print(f"{'读取时间 (秒)':<20} {csv_read_time:>12.4f} {parquet_read_time:>12.4f} {csv_read_time/parquet_read_time:>11.1f}x")
print(f"{'文件大小 (MB)':<20} {csv_size:>12.2f} {parquet_size:>12.2f} {csv_size/parquet_size:>11.1f}x")

# 清理
"""
os.remove(csv_path)
os.remove(parquet_path)
"""
print("\n✅ 所有任务完成！")