# Financial Data Pipeline
## Modules

### 1. DataProcessor (`processor.py`)
- Load CSV files from multiple sources
- Align prices across tickers
- Calculate returns matrix

### 2. RiskMetrics (`risk_metrics.py`)
- Rolling volatility, Sharpe ratio, max drawdown
- EWMA volatility (RiskMetrics model)
- Rolling Beta calculation
- VaR (Value at Risk)

### 3. RiskReport (`risk_metrics.py`)
- Generate daily risk summary
- High volatility detection
- HTML report export

## Quick Start

```python
from processor import DataProcessor
from risk_metrics import RiskMetrics, RiskReport

# Load data
dp = DataProcessor('./data')
dp.load_raw()
returns = dp.process_returns()

# Calculate metrics
rm = RiskMetrics(returns)
vol = rm.ewma_volatility()
beta = rm.rolling_beta('AAPL', 'GOOGL')

# Generate report
report = RiskReport(rm)
summary = report.generate_summary()
print(summary)

well，actually，实际应用中，一定要用连续的，和移动加权（ewm)

# Financial Data Pipeline

## Modules

### 1. DataProcessor (`processor.py`)
- Load CSV files, align prices, calculate returns

### 2. RiskMetrics (`risk_metrics.py`)
- Rolling volatility, Sharpe, max drawdown
- EWMA volatility, Beta calculation, VaR

### 3. FactorBacktest (`factor_backtest.py`)

    - Momentum, volatility, MA crossover factors
    - Signal generation with configurable thresholds
    - Vectorized strategy backtesting
    - IC/IR factor evaluation
    - Multi-factor combination
    - Commission and turnover modeling
    - Performance visualization
    - Parameter sensitivity analysis

## Quick Start

```python
from processor import DataProcessor
from factor_backtest import FactorBacktest

# Load
dp = DataProcessor('./data')
dp.load_raw()
prices = dp.process_prices()

# Backtest
bt = FactorBacktest(prices)
mom = bt.momentum_factor(20)
signals = bt.generate_signals(mom, long_threshold=0.02)
returns = bt.calculate_strategy_returns(signals)
perf = bt.calculate_performance()

# Visualize
bt.plot_equity_curve()
bt.plot_drawdown()