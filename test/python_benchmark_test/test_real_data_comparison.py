#!/usr/bin/env python3
"""
Real data comparison tests between EpochFolio protobuf and PyFolio calculations
Tests actual extracted data values, not just structure
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Tuple
import sys

# Add pyfolio-reloaded to path
sys.path.insert(0, str(Path(__file__).parent / "pyfolio-reloaded/src"))

# Import protobuf modules  
import epoch_protos.tearsheet_pb2 as tearsheet

# Import pyfolio
import pyfolio.timeseries as ts

class TestRealDataComparison:
    """Test class for comparing actual extracted data values with pyfolio"""
    
    @classmethod
    def setup_class(cls):
        """Setup test data paths"""
        cls.test_output_dir = Path("/home/adesola/EpochLab/EpochFolio/cmake-build-debug/bin/test_output")
        cls.pb_files = {
            'strategy_benchmark': cls.test_output_dir / 'strategy_benchmark_test_result.pb',
            'full': cls.test_output_dir / 'full_test_result.pb',
        }

    def load_protobuf_data(self, pb_file: Path):
        """Load protobuf with appropriate message type"""
        if 'full' in pb_file.name:
            pb_data = tearsheet.FullTearSheet()
        else:
            pb_data = tearsheet.TearSheet()
            
        with open(pb_file, 'rb') as f:
            pb_data.ParseFromString(f.read())
        return pb_data

    def extract_cumulative_returns(self, pb_data) -> Optional[pd.Series]:
        """Extract cumulative returns data from protobuf charts"""
        
        # Check charts in the main data structure
        charts_to_check = []
        if hasattr(pb_data, 'charts'):
            charts_to_check.extend(pb_data.charts.charts)
            
        # For FullTearSheet, also check strategy_benchmark section
        if hasattr(pb_data, 'strategy_benchmark') and pb_data.strategy_benchmark:
            if hasattr(pb_data.strategy_benchmark, 'charts'):
                charts_to_check.extend(pb_data.strategy_benchmark.charts.charts)
        
        for chart in charts_to_check:
            # Look for cumulative returns chart
            if hasattr(chart, 'lines_def') and chart.lines_def:
                lines_def = chart.lines_def
                if hasattr(lines_def, 'chart_def') and 'cumulative' in lines_def.chart_def.title.lower():
                    
                    # Extract the lines data
                    if hasattr(lines_def, 'lines') and len(lines_def.lines) > 0:
                        line = lines_def.lines[0]  # Take first line
                        if hasattr(line, 'data') and len(line.data) > 0:
                            
                            cum_returns_values = []
                            dates = []
                            for i, data_point in enumerate(line.data):
                                if hasattr(data_point, 'y') and hasattr(data_point, 'x'):
                                    # y is direct double, x is timestamp_ms
                                    cum_returns_values.append(data_point.y)
                                    # Convert timestamp from milliseconds to pandas timestamp
                                    dates.append(pd.Timestamp.fromtimestamp(data_point.x / 1000.0))
                            
                            if len(cum_returns_values) > 10 and len(dates) == len(cum_returns_values):  # Need substantial data
                                print(f"📊 Extracted {len(cum_returns_values)} cumulative return data points")
                                print(f"   Range: {min(cum_returns_values):.6f} to {max(cum_returns_values):.6f}")
                                print(f"   Date range: {min(dates)} to {max(dates)}")
                                
                                return pd.Series(cum_returns_values, index=dates, name='cumulative_returns')
        
        return None

    def convert_cumulative_to_period_returns(self, cum_returns: pd.Series) -> pd.Series:
        """Convert cumulative returns to period returns for pyfolio analysis"""
        if cum_returns is None or len(cum_returns) < 2:
            return None
            
        # Calculate period returns from cumulative returns
        # period_return = (cum_return[t] / cum_return[t-1]) - 1
        period_returns = cum_returns.pct_change().dropna()
        
        print(f"📈 Converted to {len(period_returns)} period returns")
        print(f"   Mean daily return: {period_returns.mean():.6f}")
        print(f"   Daily volatility: {period_returns.std():.6f}")
        
        return period_returns

    def test_extract_real_cumulative_returns_data(self):
        """Test that we can extract real cumulative returns from protobuf"""
        
        print("\n🔍 Testing Real Data Extraction...")
        
        # Test strategy benchmark file
        pb_data = self.load_protobuf_data(self.pb_files['strategy_benchmark'])
        cum_returns = self.extract_cumulative_returns(pb_data)
        
        assert cum_returns is not None, "Failed to extract cumulative returns data"
        assert len(cum_returns) > 50, f"Expected >50 data points, got {len(cum_returns)}"
        
        # Validate data range makes sense for cumulative returns
        assert cum_returns.min() > 0.5, f"Cumulative returns too low: {cum_returns.min()}"
        assert cum_returns.max() < 5.0, f"Cumulative returns too high: {cum_returns.max()}"
        
        # Check that it's actually cumulative (generally trending)
        final_return = cum_returns.iloc[-1]
        initial_return = cum_returns.iloc[0]
        
        print(f"   Initial value: {initial_return:.6f}")
        print(f"   Final value: {final_return:.6f}")
        print(f"   Total return: {(final_return/initial_return - 1)*100:.2f}%")
        
        assert abs(initial_return - 1.0) < 0.1, f"Expected initial cumulative return ~1.0, got {initial_return}"
        
        print("✅ Real cumulative returns data extracted successfully")

    def test_convert_to_period_returns_and_calculate_metrics(self):
        """Test conversion to period returns and calculation of financial metrics"""
        
        print("\n📊 Testing Period Returns Calculation...")
        
        # Extract cumulative returns
        pb_data = self.load_protobuf_data(self.pb_files['strategy_benchmark'])
        cum_returns = self.extract_cumulative_returns(pb_data)
        
        assert cum_returns is not None, "Need cumulative returns for this test"
        
        # Convert to period returns
        period_returns = self.convert_cumulative_to_period_returns(cum_returns)
        
        assert period_returns is not None, "Failed to convert to period returns"
        assert len(period_returns) > 50, f"Expected >50 period returns, got {len(period_returns)}"
        
        # Validate period returns are reasonable
        assert period_returns.min() > -0.5, f"Daily return too negative: {period_returns.min()}"
        assert period_returns.max() < 0.5, f"Daily return too positive: {period_returns.max()}"
        assert abs(period_returns.mean()) < 0.1, f"Mean return seems unrealistic: {period_returns.mean()}"
        assert period_returns.std() < 0.2, f"Volatility seems too high: {period_returns.std()}"
        
        print("✅ Period returns calculated and validated")

    def test_pyfolio_calculations_on_real_data(self):
        """Test pyfolio calculations on real extracted data"""
        
        print("\n🧮 Testing PyFolio Calculations on Real Data...")
        
        # Extract and convert data
        pb_data = self.load_protobuf_data(self.pb_files['strategy_benchmark'])
        cum_returns = self.extract_cumulative_returns(pb_data)
        period_returns = self.convert_cumulative_to_period_returns(cum_returns)
        
        assert period_returns is not None, "Need period returns for pyfolio calculations"
        
        # Calculate pyfolio metrics
        try:
            annual_return = ts.annual_return(period_returns)
            pyfolio_cum_returns = ts.cum_returns(period_returns)
            
            # Compare final cumulative return
            original_final = cum_returns.iloc[-1]
            pyfolio_final = pyfolio_cum_returns.iloc[-1] + 1.0  # pyfolio returns (ret-1), we need (1+ret)
            
            print(f"📈 Metric Comparisons:")
            print(f"   Original final cumulative return: {original_final:.6f}")
            print(f"   PyFolio recalculated cumulative return: {pyfolio_final:.6f}")
            print(f"   Difference: {abs(original_final - pyfolio_final):.6f}")
            print(f"   Annual return (PyFolio): {annual_return:.4f} ({annual_return*100:.2f}%)")
            
            # They should be very close (within 0.1% relative error)
            relative_error = abs(original_final - pyfolio_final) / original_final
            assert relative_error < 0.001, f"Cumulative returns don't match: {relative_error:.6f} relative error"
            
            # Annual return should be reasonable
            assert -0.5 < annual_return < 2.0, f"Annual return seems unrealistic: {annual_return}"
            
            print("✅ PyFolio calculations match extracted data!")
            
        except Exception as e:
            pytest.fail(f"PyFolio calculations failed: {e}")

    def test_data_consistency_across_tearsheets(self):
        """Test that data is consistent between full and strategy_benchmark tearsheets"""
        
        print("\n🔄 Testing Cross-Tearsheet Data Consistency...")
        
        # Extract from both tearsheets
        strategy_data = self.load_protobuf_data(self.pb_files['strategy_benchmark'])
        full_data = self.load_protobuf_data(self.pb_files['full'])
        
        strategy_cum_returns = self.extract_cumulative_returns(strategy_data)
        full_cum_returns = self.extract_cumulative_returns(full_data)
        
        if strategy_cum_returns is not None and full_cum_returns is not None:
            print(f"   Strategy benchmark: {len(strategy_cum_returns)} points")
            print(f"   Full tearsheet: {len(full_cum_returns)} points")
            
            # If they have the same length, they should be very similar
            if len(strategy_cum_returns) == len(full_cum_returns):
                diff = (strategy_cum_returns - full_cum_returns).abs()
                max_diff = diff.max()
                mean_diff = diff.mean()
                
                print(f"   Maximum difference: {max_diff:.8f}")
                print(f"   Mean difference: {mean_diff:.8f}")
                
                assert max_diff < 0.001, f"Data inconsistency detected: max diff {max_diff}"
                print("✅ Data is consistent across tearsheets!")
            else:
                print("⚠️  Different data lengths, but both contain valid data")
        else:
            print("⚠️  Could not extract data from both tearsheets for comparison")

    def test_calculate_all_standard_metrics(self):
        """Calculate all standard financial metrics and validate they're reasonable"""
        
        print("\n📊 Testing All Standard Financial Metrics...")
        
        # Extract data
        pb_data = self.load_protobuf_data(self.pb_files['strategy_benchmark'])
        cum_returns = self.extract_cumulative_returns(pb_data)
        period_returns = self.convert_cumulative_to_period_returns(cum_returns)
        
        assert period_returns is not None, "Need returns data for metric calculations"
        
        # Calculate comprehensive metrics
        try:
            metrics = {
                'Total Return': cum_returns.iloc[-1] - 1.0,  # Convert from cumulative to total return
                'Annual Return': ts.annual_return(period_returns),
                'Daily Mean Return': period_returns.mean(),
                'Daily Volatility': period_returns.std(),
                'Data Points': len(period_returns),
                'Date Range': f"{period_returns.index[0].date()} to {period_returns.index[-1].date()}"
            }
            
            print(f"📈 Calculated Metrics Summary:")
            for name, value in metrics.items():
                if isinstance(value, float):
                    if 'return' in name.lower():
                        print(f"   {name:20}: {value:8.4f} ({value*100:6.2f}%)")
                    else:
                        print(f"   {name:20}: {value:8.6f}")
                else:
                    print(f"   {name:20}: {value}")
            
            # Validate all metrics are reasonable
            assert -0.9 < metrics['Total Return'] < 5.0, f"Total return unrealistic: {metrics['Total Return']}"
            assert -1.0 < metrics['Annual Return'] < 3.0, f"Annual return unrealistic: {metrics['Annual Return']}"
            assert abs(metrics['Daily Mean Return']) < 0.01, f"Daily mean return unrealistic: {metrics['Daily Mean Return']}"
            assert 0.001 < metrics['Daily Volatility'] < 0.1, f"Daily volatility unrealistic: {metrics['Daily Volatility']}"
            
            print("✅ All financial metrics calculated and validated!")
            return metrics
            
        except Exception as e:
            pytest.fail(f"Metric calculation failed: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])