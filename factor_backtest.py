"""
Factor Backtest Module
Author: [你的名字]
Date: 2026-04-24
Description: Vectorized factor calculation and strategy backtesting
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Callable
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from scipy.stats import spearmanr
class FactorBacktest:
    """
    向量化因子回测框架
    - 因子计算（动量、波动率、均线交叉等）
    - 信号生成（多空阈值）
    - 策略收益计算
    - 回测绩效评估
    """
    
    def __init__(self, prices: pd.DataFrame):
        """
        parameters:
        prices: DataFrame, 行=日期, 列=资产, 值=收盘价
        """
        self.prices = prices
        self.returns = np.log(prices/prices.shift(1))
        self.signals = None
        self.strategy_returns = None
        
    def momentum_factor(self, window: int = 20) -> pd.DataFrame:
        """
        计算动量因子
        window: 回溯窗口（交易日）
        返回: DataFrame，值 = (当前价 / N日前价) - 1
        """
        return self.prices / self.prices.shift(window) - 1
    
    def volatility_factor(self, window: int = 20) -> pd.DataFrame:
        """
        计算波动率因子
        返回: 年化波动率
        """
        return self.returns.rolling(window).std(ddof=0) * np.sqrt(252)
    
    def ma_crossover_factor(self, fast: int = 5, slow: int = 20) -> pd.DataFrame:
        """
        均线交叉因子
        返回: 快线 / 慢线 - 1（正值表示快线在上）
        """
        ma_fast = self.prices.rolling(fast).mean()
        ma_slow = self.prices.rolling(slow).mean()
        return ma_fast / ma_slow - 1
    
    def generate_signals(self, factor: pd.DataFrame, 
                         long_threshold: float = 0.0, 
                         short_threshold: Optional[float] = None) -> pd.DataFrame:
        """
        根据因子值生成交易信号
        factor: 因子值 DataFrame
        long_threshold: 做多阈值（因子大于此值做多）
        short_threshold: 做空阈值（因子小于此值做空），None 表示不做空
        """
        if short_threshold is None:
            short_threshold = -long_threshold
        
        conditions = [
            factor > long_threshold,
            factor < short_threshold
        ]
        choices = [1, -1]
        
        self.signals = pd.DataFrame(
            np.select(conditions, choices, default=0),
            index=factor.index,
            columns=factor.columns
        )
        return self.signals
    
    def calculate_strategy_returns(self, signals: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        计算策略收益率
        策略收益 = 信号(t-1) * 实际收益(t)  # 用前一天的信号，因为今天开盘前决定仓位
        """
        if signals is not None:
            self.signals = signals
        
        if self.signals is None:
            raise ValueError("请先调用 generate_signals() 或传入 signals")
        
        # 关键：信号滞后一天（今天收盘的信号，明天开盘才能交易）
        self.strategy_returns = self.signals.shift(1) * self.returns
        return self.strategy_returns
    
    def calculate_performance(self) -> Dict:
        """
        计算回测绩效指标
        返回: 字典，包含年化收益、波动率、夏普、最大回撤等
        """
        if self.strategy_returns is None:
            raise ValueError("请先调用 calculate_strategy_returns()")
        
        # 等权组合收益率
        portfolio_returns = self.strategy_returns.sum(axis=1)
        
        # 累积净值
        cum_returns = portfolio_returns.cumsum().apply(np.exp)
        total_log_return = portfolio_returns.sum()
        n_years = len(portfolio_returns) / 252
        annual_return = np.exp(total_log_return / n_years) - 1
    
        # total_return 用于最终展示
        total_return = cum_returns.iloc[-1] - 1
        
        # 年化波动率
        annual_vol = portfolio_returns.std(ddof=0) * np.sqrt(252)
        
        # 夏普比率（假设无风险利率为0）
        sharpe = annual_return / annual_vol if annual_vol > 0 else 0
        
        # 最大回撤
        cummax = cum_returns.cummax()
        drawdowns = (cum_returns - cummax) / cummax
        max_drawdown = drawdowns.min()
        
        # 胜率
        win_rate = (portfolio_returns > 0).sum() / portfolio_returns.count()
        
        # 盈亏比
        avg_win = portfolio_returns[portfolio_returns > 0].mean()
        avg_loss = portfolio_returns[portfolio_returns < 0].mean()
        profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else np.inf
        
        performance = {
            'annual_return': annual_return,
            'annual_volatility': annual_vol,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio,
            'total_return': total_return,
            'cum_returns': cum_returns
        }
        
        return performance

    
    def ema_crossover_signal(self, fast: int = 5, slow: int = 20) -> pd.DataFrame:
        """均线交叉信号：快线在上做多，否则空仓"""
        ema_fast = self.prices.ewm(span = fast,adjust = False).mean()
        ema_slow = self.prices.ewm(span = slow,adjust = False).mean()
        conditions = [
            ema_fast>ema_slow,
            ema_fast<ema_slow,
        ]
        choices = [1,-1]
        signals = pd.DataFrame(
            np.select(conditions,choices,default = 0),
            index = self.prices.index,
            columns = self.prices.columns
        )
        return signals

    def apply_signal_delay(self, signals: pd.DataFrame, lag: int = 1) -> pd.DataFrame:
        """应用信号延迟（避免未来函数）"""
        return signals.shift(lag)

    def calculate_turnover(self, signals: pd.DataFrame) -> pd.Series:
        """计算每日换手率"""
        changes = signals.diff().abs().sum(axis=1)
        return changes / signals.shape[1]

    def calculate_commission(self, signals: pd.DataFrame, rate: float = 0.0015) -> pd.DataFrame:
        """计算交易手续费"""
        trades = signals.diff().abs()
        return trades * rate

    def calculate_net_returns(self, signals: Optional[pd.DataFrame] = None, 
                              commission_rate: float = 0.0015) -> pd.DataFrame:
        """计算扣除手续费后的净收益"""
        if signals is not None:
            self.signals = signals
    
        # 毛收益
        gross_returns = self.calculate_strategy_returns()
    
        # 手续费
        commission = self.calculate_commission(self.signals, commission_rate)
    
        # 净收益
        return gross_returns - commission

    def calculate_performance_net(self, commission_rate: float = 0.0015) -> Dict:
        """计算扣除手续费后的绩效"""
        net_returns = self.calculate_net_returns(commission_rate=commission_rate)
    
        # 临时替换 strategy_returns
        original_returns = self.strategy_returns
        self.strategy_returns = net_returns
        perf = self.calculate_performance()
        self.strategy_returns = original_returns  # 恢复
    
        return perf

    

    def calculate_ic(self, factor: pd.DataFrame, 
                     forward_returns: Optional[pd.DataFrame] = None) -> pd.Series:
        """
            计算因子的 Rank IC 序列（Spearman 相关性）
    
            Parameters:
            factor: 因子值，行=日期，列=股票
            forward_returns: 下期对数收益率，默认用 self.returns.shift(-1)
    
            Returns:
            Series: 每日 IC 值
        """
        if forward_returns is None:
            # self.returns 已是对数收益率（连续复利）
            forward_returns = self.returns.shift(-1)
    
        ic_values = {}
    
        for date in factor.index[:-1]:  # 最后一天没有下期收益
            f = factor.loc[date].dropna()
            if len(f) < 10:
                continue
        
            # 找到下一个交易日
            next_dates = forward_returns.index[forward_returns.index > date]
            if len(next_dates) == 0:
                    break
            next_date = next_dates[0]
        
            r = forward_returns.loc[next_date].dropna()
        
        # 取交集
            common = f.index.intersection(r.index)
            if len(common) >= 10:
                ic, _ = spearmanr(f[common], r[common])
                ic_values[date] = ic
    
        return pd.Series(ic_values, name='IC')


    def calculate_ic_summary(self, factor: pd.DataFrame) -> Dict:
        """计算因子 IC 摘要统计"""
        ic_series = self.calculate_ic(factor)
    
        n = len(ic_series)
        ic_mean = ic_series.mean()
        ic_std = ic_series.std(ddof=0)  # 总体标准差（与波动率计算一致）
    
        return {
            'ic_mean': ic_mean,
            'ic_std': ic_std,
            'ic_ir': ic_mean / ic_std if ic_std > 0 else 0,  # Information Ratio
            'ic_win_rate': (ic_series > 0).mean(),           # IC>0 的比例
            'ic_t_stat': ic_mean / ic_std * np.sqrt(n) if ic_std > 0 else 0,  # t统计量
            'ic_positive_days': (ic_series > 0).sum(),
            'ic_negative_days': (ic_series < 0).sum(),
            'n_obs': n
        }


    def combine_factors_equal_weight(self, factors: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        等权组合多个因子（横截面标准化后加总）
        
        Parameters:
        factors: 字典 {因子名: 因子值DataFrame}
    
        Returns:
        DataFrame: 组合因子值
        """
        # 初始化（用第一个因子的结构）
        first_factor = list(factors.values())[0]
        combined = pd.DataFrame(0.0, index=first_factor.index, columns=first_factor.columns)
    
        for name, factor in factors.items():
            # 横截面标准化：每天的所有股票中，均值=0，标准差=1
            # axis=1 表示对每行（每个日期）计算跨股票的统计量
            zscore = factor.sub(factor.mean(axis=1), axis=0).div(factor.std(axis=1), axis=0)
            combined = combined + zscore.fillna(0)  # 处理 NaN
    
        return combined / len(factors)




    def plot_equity_curve(self, benchmark: bool = True, figsize: tuple = (12, 6)):
        """绘制净值曲线（连续复利版本）"""
        perf = self.calculate_performance()
        cum_returns = perf['cum_returns']
        
        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(cum_returns.index, cum_returns.values, 'b-', linewidth=1.5, label='Strategy')
        
        if benchmark:
            # 买入持有（连续复利）
            bh_log_returns = self.returns.mean(axis=1)
            cum_bh = bh_log_returns.cumsum().apply(np.exp)
            ax.plot(cum_bh.index, cum_bh.values, 'gray', linewidth=1, alpha=0.7, label='Buy & Hold')
        
        ax.axhline(y=1, color='black', linestyle='--', alpha=0.5)
        ax.legend()
        ax.set_title('Strategy Equity Curve')
        ax.set_ylabel('Net Value')
        ax.grid(True, alpha=0.3)
        
        return fig
    
    
    def plot_drawdown(self, figsize: tuple = (12, 4)):
        """绘制回撤图"""
        perf = self.calculate_performance()
        cum_returns = perf['cum_returns']
        cummax = cum_returns.cummax()
        drawdowns = (cum_returns - cummax) / cummax * 100
        
        fig, ax = plt.subplots(figsize=figsize)
        ax.fill_between(drawdowns.index, 0, drawdowns.values, color='red', alpha=0.5)
        ax.set_title('Strategy Drawdown')
        ax.set_ylabel('Drawdown (%)')
        ax.grid(True, alpha=0.3)
        
        return fig
    
    
    def plot_monthly_heatmap(self, figsize: tuple = (12, 5)):
        """绘制月度收益热力图（连续复利版本）"""
        if self.strategy_returns is None:
            raise ValueError("请先计算策略收益")
        
        # 组合对数收益
        portfolio_log_returns = self.strategy_returns.sum(axis=1)
        
        # 对数收益按月求和
        monthly_log_returns = portfolio_log_returns.resample('ME').sum()
        
        # 转回简单收益率用于热力图展示
        monthly_returns = monthly_log_returns.apply(lambda x: np.exp(x) - 1)
        monthly_matrix = monthly_returns.groupby(
            [monthly_returns.index.year, monthly_returns.index.month]
        ).first().unstack()
        
        if monthly_matrix.empty:
            return None
        
        fig, ax = plt.subplots(figsize=figsize)
        im = ax.imshow(monthly_matrix.values, cmap='RdYlGn', aspect='auto',
                       vmin=-0.1, vmax=0.1)
        
        # 设置标签
        months = ['Jan','Feb','Mar','Apr','May','Jun',
                  'Jul','Aug','Sep','Oct','Nov','Dec']
        ax.set_xticks(range(len(monthly_matrix.columns)))
        ax.set_xticklabels(months[:len(monthly_matrix.columns)])
        ax.set_yticks(range(len(monthly_matrix.index)))
        ax.set_yticklabels(monthly_matrix.index)
        ax.set_title('Monthly Returns Heatmap')
        
        plt.colorbar(im, ax=ax, format=mtick.FuncFormatter(lambda x, _: f'{x:.0%}'))
        
        return fig
    
    
    def plot_full_report(self, title: str = 'Strategy Backtest Report',
                         figsize: tuple = (14, 12)):
        """生成完整回测报告（三合一）"""
        perf = self.calculate_performance()
        cum_returns = perf['cum_returns']
        portfolio_log_returns = self.strategy_returns.sum(axis=1)
        
        fig, axes = plt.subplots(3, 1, figsize=figsize)
        
        # 1. 净值曲线
        axes[0].plot(cum_returns.index, cum_returns.values, 'b-', linewidth=1.5)
        axes[0].fill_between(cum_returns.index, 1, cum_returns.values,
                              where=(cum_returns.values >= 1), color='green', alpha=0.3)
        axes[0].fill_between(cum_returns.index, 1, cum_returns.values,
                              where=(cum_returns.values < 1), color='red', alpha=0.3)
        axes[0].axhline(y=1, color='black', linestyle='--', alpha=0.5)
        axes[0].set_title(f'{title} - Equity Curve')
        axes[0].set_ylabel('Net Value')
        axes[0].grid(True, alpha=0.3)
        
        # 2. 回撤图
        cummax = cum_returns.cummax()
        drawdowns = (cum_returns - cummax) / cummax * 100
        axes[1].fill_between(drawdowns.index, 0, drawdowns.values, color='red', alpha=0.5)
        axes[1].set_title('Drawdown (%)')
        axes[1].set_ylabel('Drawdown %')
        axes[1].grid(True, alpha=0.3)
        
        # 3. 月度收益热力图
        monthly_log_returns = portfolio_log_returns.resample('ME').sum()
        monthly_returns = monthly_log_returns.apply(lambda x: np.exp(x) - 1)
        monthly_matrix = monthly_returns.groupby(
            [monthly_returns.index.year, monthly_returns.index.month]
        ).first().unstack()
        
        if not monthly_matrix.empty:
            im = axes[2].imshow(monthly_matrix.values, cmap='RdYlGn', aspect='auto',
                               vmin=-0.1, vmax=0.1)
            months = ['Jan','Feb','Mar','Apr','May','Jun',
                      'Jul','Aug','Sep','Oct','Nov','Dec']
            axes[2].set_xticks(range(len(monthly_matrix.columns)))
            axes[2].set_xticklabels(months[:len(monthly_matrix.columns)])
            axes[2].set_yticks(range(len(monthly_matrix.index)))
            axes[2].set_yticklabels(monthly_matrix.index)
            axes[2].set_title('Monthly Returns Heatmap')
            plt.colorbar(im, ax=axes[2], format=mtick.FuncFormatter(lambda x, _: f'{x:.0%}'))
        
        plt.tight_layout()
        return fig
    
    
    def parameter_sensitivity(self, param_name: str, param_range: list,
                              factor_func: Callable, threshold: float = 0,
                              **kwargs) -> pd.DataFrame:
        """
        参数敏感性分析（EWMA 友好版本）
        
        Parameters:
        param_name: 参数名称（如 'span', 'window'）
        param_range: 参数取值范围
        factor_func: 因子计算函数，接受参数返回因子 DataFrame
        threshold: 多空阈值（默认 0）
        """
        results = []
        
        for param_val in param_range:
            # 计算因子
            factor = factor_func(param_val, **kwargs)
            
            # 生成信号
            signals = self.generate_signals(factor, long_threshold=threshold)
            
            # 计算策略收益
            self.calculate_strategy_returns(signals)
            
            # 计算绩效
            perf = self.calculate_performance()
            
            results.append({
                param_name: param_val,
                'sharpe': perf['sharpe_ratio'],
                'annual_return': perf['annual_return'],
                'max_drawdown': perf['max_drawdown'],
                'win_rate': perf['win_rate']
            })
        
        return pd.DataFrame(results)
    
    
    # ============================================================
    # 使用 EWMA 的因子函数示例
    # ============================================================
    
    def momentum_factor_ewma(span: int, **kwargs) -> pd.DataFrame:
        """EWMA 动量因子：价格 / EWMA 均价 - 1"""
        prices = kwargs.get('prices')  # 需要传入 prices
        ema = prices.ewm(span=span, adjust=False).mean()
        return prices / ema - 1
    
    # 使用示例：
    # bt = FactorBacktest(prices)
    # results = bt.parameter_sensitivity(
    #     'span', range(5, 125, 5),
    #     lambda s: bt.prices / bt.prices.ewm(span=s, adjust=False).mean() - 1
    # )