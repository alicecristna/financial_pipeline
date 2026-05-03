import sys
import pandas as pd
import numpy as np
from pathlib import Path

# 禁止生成缓存文件
sys.dont_write_bytecode = True

# 添加模块路径
module_path = str(Path.home() / 'Desktop' / '代码' / 'data analysis' / '新征程')
sys.path.append(module_path)

from processor import DataProcessor
from factor_backtest import FactorBacktest

# ============================================================
# 1. 加载数据
# ============================================================
dp = DataProcessor('/Users/yuanli/Desktop/代码/data analysis/新征程/data')
dp.load_raw()
prices = dp.process_prices()
returns = dp.process_returns()

print(f"数据形状: {prices.shape}")
print(f"日期范围: {prices.index[0]} ~ {prices.index[-1]}")

# ============================================================
# 2. 初始化回测框架
# ============================================================
bt = FactorBacktest(prices)

# ============================================================
# 3. 计算因子
# ============================================================
mom_20 = bt.momentum_factor(window=20)
vol_20 = bt.volatility_factor(window=20)

# 检查因子值范围
print(f"\n动量因子范围: {mom_20.min().min():.4f} ~ {mom_20.max().max():.4f}")
print(f"波动率因子范围: {vol_20.min().min():.4f} ~ {vol_20.max().max():.4f}")

# ============================================================
# 4. 评估因子质量
# ============================================================
ic_mom = bt.calculate_ic(mom_20)
ic_summary_mom = bt.calculate_ic_summary(mom_20)

print("\n=== 动量因子 IC 分析 ===")
for k, v in ic_summary_mom.items():
    print(f"  {k}: {v:.4f}")

# ============================================================
# 5. 多因子组合
# ============================================================
factors = {
    'momentum': mom_20,
    'low_vol': -vol_20  # 低波动因子（波动率越低越好）
}
composite = bt.combine_factors_equal_weight(factors)

# ============================================================
# 6. 生成信号（修正：用更合理的阈值）
# ============================================================
# 方法1：用分位数（推荐）
threshold = mom_20.quantile(0.8, axis=1).mean()  # 用 80% 分位数
signals_mom = bt.generate_signals(mom_20, long_threshold=threshold.item())

print(f"\n动量信号分布:")
print(f"  做多: {(signals_mom == 1).sum().sum()}")
print(f"  做空: {(signals_mom == -1).sum().sum()}")
print(f"  空仓: {(signals_mom == 0).sum().sum()}")

# ============================================================
# 7. 回测对比
# ============================================================
# 动量策略
bt.calculate_strategy_returns(signals_mom)
perf_mom = bt.calculate_performance()

# 复合因子策略
signals_composite = bt.generate_signals(composite, long_threshold=0)
bt.calculate_strategy_returns(signals_composite)
perf_composite = bt.calculate_performance()

# ============================================================
# 8. 结果展示
# ============================================================
print("\n=== 策略绩效对比 ===")
print(f"{'指标':<15} {'动量':<12} {'复合':<12}")
print("-" * 40)
for key in ['annual_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']:
    print(f"{key:<15} {perf_mom[key]:<12.4%} {perf_composite[key]:<12.4%}")

# ============================================================
# 9. 可视化
# ============================================================
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(perf_mom['cum_returns'], label='Momentum', alpha=0.8)
ax.plot(perf_composite['cum_returns'], label='Composite', alpha=0.8)
ax.legend()
ax.set_title('Strategy Comparison')
ax.set_ylabel('Cumulative Return')
plt.tight_layout()
plt.show()