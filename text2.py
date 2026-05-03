# ============================================================
# FactorBacktest 可视化测试代码
# ============================================================

import sys
from pathlib import Path

# 添加模块路径
sys.path.append(str(Path.home() / 'Desktop' / '代码' / 'data analysis' / '新征程'))

from processor import DataProcessor
from factor_backtest import FactorBacktest
import matplotlib.pyplot as plt

# ============================================================
# 1. 加载数据
# ============================================================
dp = DataProcessor('/Users/yuanli/Desktop/代码/data analysis/新征程/data')
dp.load_raw()
prices = dp.process_prices()

print(f"数据: {prices.shape[1]} 只股票, {prices.shape[0]} 个交易日")
print(f"日期: {prices.index[0].date()} ~ {prices.index[-1].date()}")

# ============================================================
# 2. 初始化回测框架
# ============================================================
bt = FactorBacktest(prices)

# ============================================================
# 3. 生成策略：EWMA 动量 + EMA 交叉信号
# ============================================================
ema_signal = bt.ema_crossover_signal(fast=5, slow=20)
ema_signal_delayed = bt.apply_signal_delay(ema_signal)
bt.calculate_strategy_returns(ema_signal_delayed)

print(f"\n信号分布:")
print(f"  做多: {(ema_signal_delayed == 1).sum().sum()}")
print(f"  做空: {(ema_signal_delayed == -1).sum().sum()}")
print(f"  空仓: {(ema_signal_delayed == 0).sum().sum()}")

# ============================================================
# 4. 测试单个图表
# ============================================================

# 4.1 净值曲线（含基准对比）
fig1 = bt.plot_equity_curve(benchmark=True)
fig1.suptitle('EMA(5,20) Crossover Strategy', fontsize=14)
plt.show()

# 4.2 回撤图
fig2 = bt.plot_drawdown()
plt.show()

# 4.3 月度热力图
fig3 = bt.plot_monthly_heatmap()
if fig3:
    plt.show()

# ============================================================
# 5. 测试完整报告（三合一）
# ============================================================
fig4 = bt.plot_full_report('EMA(5,20) Crossover - Full Report')
plt.show()

# ============================================================
# 6. 测试参数敏感性分析
# ============================================================

# 6.1 EWMA 动量因子敏感性
print("\n=== EWMA 动量参数敏感性 ===")
results_mom = bt.parameter_sensitivity(
    'span', 
    param_range=[5, 10, 20, 30, 40, 60, 80, 100, 120],
    factor_func=lambda s: bt.prices / bt.prices.ewm(span=s, adjust=False).mean() - 1,
    threshold=0
)

print(results_mom.to_string(index=False))
print(f"\n最佳 span: {results_mom.loc[results_mom['sharpe'].idxmax(), 'span']} 天")

# 6.2 可视化敏感性结果
fig5, axes = plt.subplots(2, 2, figsize=(12, 8))

axes[0, 0].plot(results_mom['span'], results_mom['sharpe'], 'b-o')
axes[0, 0].set_title('Sharpe Ratio vs Span')
axes[0, 0].set_xlabel('EWMA Span')
axes[0, 0].set_ylabel('Sharpe')
axes[0, 0].grid(True, alpha=0.3)

axes[0, 1].plot(results_mom['span'], results_mom['annual_return'], 'g-o')
axes[0, 1].set_title('Annual Return vs Span')
axes[0, 1].set_xlabel('EWMA Span')
axes[0, 1].set_ylabel('Annual Return')
axes[0, 1].grid(True, alpha=0.3)

axes[1, 0].plot(results_mom['span'], results_mom['max_drawdown'], 'r-o')
axes[1, 0].set_title('Max Drawdown vs Span')
axes[1, 0].set_xlabel('EWMA Span')
axes[1, 0].set_ylabel('Max Drawdown')
axes[1, 0].grid(True, alpha=0.3)

axes[1, 1].plot(results_mom['span'], results_mom['win_rate'], 'm-o')
axes[1, 1].set_title('Win Rate vs Span')
axes[1, 1].set_xlabel('EWMA Span')
axes[1, 1].set_ylabel('Win Rate')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# ============================================================
# 7. 多策略对比
# ============================================================

strategies = {
    'EMA(5,20)': bt.ema_crossover_signal(fast=5, slow=20),
    'EMA(10,30)': bt.ema_crossover_signal(fast=10, slow=30),
    'EMA(20,60)': bt.ema_crossover_signal(fast=20, slow=60),
}

fig6, axes = plt.subplots(1, 2, figsize=(14, 5))

for name, signal in strategies.items():
    signal_delayed = bt.apply_signal_delay(signal)
    bt.calculate_strategy_returns(signal_delayed)
    perf = bt.calculate_performance()
    
    # 净值曲线
    axes[0].plot(perf['cum_returns'], label=name, alpha=0.8)
    
    # 回撤
    cummax = perf['cum_returns'].cummax()
    drawdown = (perf['cum_returns'] - cummax) / cummax * 100
    axes[1].fill_between(drawdown.index, 0, drawdown.values, alpha=0.3, label=name)

axes[0].axhline(y=1, color='black', linestyle='--', alpha=0.5)
axes[0].set_title('Strategy Comparison - Equity Curve')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

axes[1].set_title('Strategy Comparison - Drawdown')
axes[1].legend()
axes[1].grid(True, alpha=0.3)
axes[1].set_ylabel('Drawdown (%)')

plt.tight_layout()
plt.show()

# ============================================================
# 8. 手续费影响分析
# ============================================================

ema_signal = bt.ema_crossover_signal(fast=5, slow=20)
ema_signal_delayed = bt.apply_signal_delay(ema_signal)

# 不同手续费率下的绩效
rates = [0, 0.0005, 0.001, 0.0015, 0.002, 0.003]
sharpe_by_rate = []
return_by_rate = []

for rate in rates:
    bt.calculate_strategy_returns(ema_signal_delayed)
    perf_net = bt.calculate_performance_net(commission_rate=rate)
    sharpe_by_rate.append(perf_net['sharpe_ratio'])
    return_by_rate.append(perf_net['annual_return'])

fig7, ax1 = plt.subplots(figsize=(10, 5))

ax1.plot(rates, sharpe_by_rate, 'b-o', label='Sharpe Ratio')
ax1.set_xlabel('Commission Rate')
ax1.set_ylabel('Sharpe Ratio', color='b')
ax1.tick_params(axis='y', labelcolor='b')

ax2 = ax1.twinx()
ax2.plot(rates, return_by_rate, 'g-o', label='Annual Return')
ax2.set_ylabel('Annual Return', color='g')
ax2.tick_params(axis='y', labelcolor='g')

plt.title('Impact of Commission on Strategy Performance')
ax1.grid(True, alpha=0.3)
plt.show()

# ============================================================
# 9. 打印最终绩效摘要
# ============================================================
bt.calculate_strategy_returns(ema_signal_delayed)
perf_final = bt.calculate_performance()
perf_net_final = bt.calculate_performance_net(commission_rate=0.0015)

print("\n" + "="*50)
print("最终绩效摘要 - EMA(5,20) Crossover")
print("="*50)
print(f"{'指标':<20} {'毛收益':>12} {'净收益(扣费)':>12}")
print("-"*50)
for key in ['annual_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']:
    gross = f"{perf_final[key]:.4%}" if 'return' in key or 'drawdown' in key or 'rate' in key else f"{perf_final[key]:.4f}"
    net = f"{perf_net_final[key]:.4%}" if 'return' in key or 'drawdown' in key or 'rate' in key else f"{perf_net_final[key]:.4f}"
    print(f"{key:<20} {gross:>12} {net:>12}")

print("\n✅ 所有测试完成！")


