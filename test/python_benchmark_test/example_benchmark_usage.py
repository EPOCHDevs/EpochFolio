#!/usr/bin/env python3
"""
Example usage of the PyFolio benchmark framework
This shows how to use the framework once epoch_protos is properly set up
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add pyfolio to path
sys.path.insert(0, str(Path(__file__).parent / "pyfolio-reloaded/src"))

def demonstrate_pyfolio_functions():
    """Demonstrate pyfolio functions that can be used for comparison"""
    print("🧮 Demonstrating PyFolio Functions")
    print("=" * 50)
    
    # Create sample returns data
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    returns = pd.Series(
        np.random.normal(0.0008, 0.02, len(dates)), 
        index=dates, 
        name='returns'
    )
    
    try:
        import pyfolio
        import pyfolio.timeseries as ts
        
        # Calculate key metrics that would be compared with protobuf data
        cum_returns = ts.cum_returns(returns)
        metrics = {
            'Total Return': cum_returns.iloc[-1],
            'Annual Return': ts.annual_return(returns),
            'Daily Mean Return': returns.mean(),
            'Daily Volatility': returns.std(),
            'Cumulative Return (Final)': cum_returns.iloc[-1],
        }
        
        print("📊 Calculated Metrics:")
        for name, value in metrics.items():
            print(f"  {name:18}: {value:8.4f}")
        
        # These are the types of calculations that would be compared
        # with the equivalent values in the protobuf tearsheets
        
        print(f"\\n📈 Sample Returns Data:")
        print(f"  Date Range: {returns.index[0].date()} to {returns.index[-1].date()}")
        print(f"  Data Points: {len(returns)}")
        print(f"  Mean Daily Return: {returns.mean():.6f}")
        print(f"  Daily Volatility: {returns.std():.6f}")
        
        return metrics
        
    except ImportError as e:
        print(f"❌ Error importing pyfolio: {e}")
        return None

def demonstrate_data_structures():
    """Show the expected data structures for comparison"""
    print("\\n📋 Expected Data Structures")
    print("=" * 50)
    
    # Returns data structure
    print("1. Returns Data:")
    print("   Expected: pd.Series with DatetimeIndex")
    print("   Example:")
    sample_returns = pd.Series(
        [0.01, -0.005, 0.02, 0.001], 
        index=pd.date_range('2023-01-01', periods=4),
        name='returns'
    )
    print(f"   {sample_returns}")
    
    # Positions data structure
    print("\\n2. Positions Data:")
    print("   Expected: Dict or DataFrame with symbol -> position")
    print("   Example:")
    sample_positions = {'AAPL': 100, 'GOOGL': 50, 'TSLA': -25}
    for symbol, position in sample_positions.items():
        print(f"   {symbol}: {position}")
    
    # Transactions data structure
    print("\\n3. Transactions Data:")
    print("   Expected: DataFrame with date, symbol, amount columns")
    print("   Example:")
    sample_transactions = pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=3),
        'symbol': ['AAPL', 'GOOGL', 'TSLA'],
        'amount': [1000, -500, 2000]
    })
    print(f"   {sample_transactions}")

def show_protobuf_integration_plan():
    """Show how protobuf data would be integrated"""
    print("\\n🔗 Protobuf Integration Plan")
    print("=" * 50)
    
    integration_steps = [
        "1. Load protobuf file using tearsheet.Tearsheet().ParseFromString()",
        "2. Extract chart data from tearsheet_data.charts",
        "3. Extract table data from tearsheet_data.tables", 
        "4. Convert to pandas structures (Series, DataFrame)",
        "5. Pass to pyfolio functions for validation",
        "6. Compare results with protobuf-stored metrics",
        "7. Assert equality within acceptable tolerance"
    ]
    
    for step in integration_steps:
        print(f"   {step}")
    
    print("\\n📝 Example Comparison:")
    print("""
   # Load protobuf tearsheet
   pb_data = tearsheet.Tearsheet()
   with open('full_test_result.pb', 'rb') as f:
       pb_data.ParseFromString(f.read())
   
   # Extract returns from protobuf
   returns_series = extract_returns_from_protobuf(pb_data)
   
   # Calculate with pyfolio
   pyfolio_sharpe = ts.sharpe_ratio(returns_series)
   
   # Get from protobuf
   pb_sharpe = find_metric_in_protobuf(pb_data, 'sharpe_ratio')
   
   # Compare
   assert abs(pyfolio_sharpe - pb_sharpe) < 0.001
    """)

def main():
    """Main demonstration function"""
    print("🚀 PyFolio Benchmark Framework Demo")
    print("🔬 Testing EpochFolio protobuf vs PyFolio calculations")
    print("=" * 70)
    
    # Demonstrate pyfolio functions
    metrics = demonstrate_pyfolio_functions()
    
    # Show expected data structures
    demonstrate_data_structures()
    
    # Show integration plan
    show_protobuf_integration_plan()
    
    print("\\n✅ Demo completed!")
    print("📖 See README.md for setup instructions")
    print("🧪 Run 'pytest test_pyfolio_benchmark.py -v' to execute tests")

if __name__ == "__main__":
    main()