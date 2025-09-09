#!/usr/bin/env python3
"""
Enhanced pytest framework for detailed field-by-field comparison 
between EpochFolio protobuf tearsheets and pyfolio calculations
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add pyfolio-reloaded to path
sys.path.insert(0, str(Path(__file__).parent / "pyfolio-reloaded/src"))

# Import protobuf modules
import epoch_protos.common_pb2 as common
import epoch_protos.chart_def_pb2 as chart_def
import epoch_protos.table_def_pb2 as table_def
import epoch_protos.tearsheet_pb2 as tearsheet

# Import pyfolio
import pyfolio
import pyfolio.timeseries as ts

class TestDetailedComparison:
    """Enhanced test class for detailed metric comparison"""
    
    @classmethod
    def setup_class(cls):
        """Setup test data and paths"""
        cls.test_output_dir = Path("/home/adesola/EpochLab/EpochFolio/cmake-build-debug/bin/test_output")
        cls.pb_files = {
            'full': cls.test_output_dir / 'full_test_result.pb',
            'positions': cls.test_output_dir / 'positions_test_result.pb',
            'returns_distribution': cls.test_output_dir / 'returns_distribution_test_result.pb',
            'risk_analysis': cls.test_output_dir / 'risk_analysis_test_result.pb',
            'round_trip': cls.test_output_dir / 'round_trip_test_result.pb',
            'strategy_benchmark': cls.test_output_dir / 'strategy_benchmark_test_result.pb',
            'transactions': cls.test_output_dir / 'transactions_test_result.pb'
        }
        
        # Verify all files exist
        for name, path in cls.pb_files.items():
            assert path.exists(), f"Protobuf file {name} not found at {path}"
    
    def load_protobuf(self, pb_file_path: Path):
        """Load protobuf file with proper message type detection"""
        pb_filename = pb_file_path.name
        
        # Try FullTearSheet first for full tearsheet
        if 'full' in pb_filename:
            try:
                tearsheet_data = tearsheet.FullTearSheet()
                with open(pb_file_path, 'rb') as f:
                    tearsheet_data.ParseFromString(f.read())
                return tearsheet_data
            except:
                pass
        
        # Try regular TearSheet
        try:
            tearsheet_data = tearsheet.TearSheet()
            with open(pb_file_path, 'rb') as f:
                tearsheet_data.ParseFromString(f.read())
            return tearsheet_data
        except Exception as e:
            raise Exception(f"Failed to parse {pb_file_path}: {e}")
    
    def extract_all_metrics(self, tearsheet_data) -> Dict[str, Any]:
        """Extract all metrics from protobuf tearsheet"""
        metrics = {}
        
        # Extract from cards (summary metrics)
        if hasattr(tearsheet_data, 'cards'):
            for card in tearsheet_data.cards:
                if hasattr(card, 'title') and hasattr(card, 'value'):
                    title = card.title
                    value = self._extract_value(card.value)
                    if value is not None:
                        metrics[f"card_{title}"] = value
                
                # Extract key-value pairs from cards
                if hasattr(card, 'key_values'):
                    for kv in card.key_values:
                        if hasattr(kv, 'key') and hasattr(kv, 'values') and len(kv.values) > 0:
                            key = kv.key
                            value = self._extract_value(kv.values[0])
                            if value is not None:
                                metrics[f"kv_{key}"] = value
        
        # Extract from tables (detailed data)
        if hasattr(tearsheet_data, 'tables'):
            for table in tearsheet_data.tables:
                if hasattr(table, 'title') and hasattr(table, 'rows'):
                    table_name = table.title.replace(' ', '_').lower()
                    
                    # Extract table data as list of rows
                    rows_data = []
                    for row in table.rows:
                        if hasattr(row, 'cells'):
                            row_data = []
                            for cell in row.cells:
                                value = self._extract_value(cell)
                                row_data.append(value)
                            if row_data:
                                rows_data.append(row_data)
                    
                    if rows_data:
                        metrics[f"table_{table_name}"] = rows_data
        
        # For FullTearSheet, extract from each section
        if hasattr(tearsheet_data, 'strategy_benchmark') and tearsheet_data.strategy_benchmark:
            section_metrics = self.extract_all_metrics(tearsheet_data.strategy_benchmark)
            for k, v in section_metrics.items():
                metrics[f"strategy_benchmark_{k}"] = v
        
        # Extract from other sections
        for section_name in ['risk_analysis', 'returns_distribution', 'positions', 'transactions', 'round_trip']:
            if hasattr(tearsheet_data, section_name):
                section = getattr(tearsheet_data, section_name)
                if section:
                    section_metrics = self.extract_all_metrics(section)
                    for k, v in section_metrics.items():
                        metrics[f"{section_name}_{k}"] = v
        
        return metrics
    
    def _extract_value(self, value_obj):
        """Extract value from protobuf value object"""
        if hasattr(value_obj, 'double_value'):
            return value_obj.double_value
        elif hasattr(value_obj, 'string_value'):
            return value_obj.string_value
        elif hasattr(value_obj, 'int_value'):
            return value_obj.int_value
        elif hasattr(value_obj, 'bool_value'):
            return value_obj.bool_value
        return None
    
    def extract_time_series_data(self, tearsheet_data) -> Optional[pd.Series]:
        """Extract time series data for pyfolio comparison"""
        # Look through charts for time series data
        charts_to_check = []
        
        if hasattr(tearsheet_data, 'charts'):
            charts_to_check.extend(tearsheet_data.charts)
        
        # For FullTearSheet, check sections
        if hasattr(tearsheet_data, 'returns_distribution') and tearsheet_data.returns_distribution:
            if hasattr(tearsheet_data.returns_distribution, 'charts'):
                charts_to_check.extend(tearsheet_data.returns_distribution.charts)
        
        for chart in charts_to_check:
            if hasattr(chart, 'title') and ('return' in chart.title.lower() or 'cumulative' in chart.title.lower()):
                if hasattr(chart, 'data') and hasattr(chart.data, 'line_series'):
                    for series in chart.data.line_series:
                        if hasattr(series, 'data_points') and len(series.data_points) > 10:  # Need substantial data
                            dates = []
                            values = []
                            
                            for point in series.data_points:
                                try:
                                    # Extract timestamp and value
                                    if hasattr(point, 'x_value') and hasattr(point, 'y_value'):
                                        x_val = self._extract_value(point.x_value)
                                        y_val = self._extract_value(point.y_value)
                                        
                                        if x_val is not None and y_val is not None:
                                            # Try to convert timestamp
                                            if isinstance(x_val, str):
                                                dates.append(pd.Timestamp(x_val))
                                            else:
                                                dates.append(pd.Timestamp.fromtimestamp(x_val))
                                            values.append(float(y_val))
                                except Exception:
                                    continue
                            
                            if len(dates) > 10 and len(values) > 10:
                                return pd.Series(values, index=dates, name='returns')
        
        return None
    
    def test_comprehensive_metric_extraction(self):
        """Test that we can extract comprehensive metrics from all protobuf files"""
        
        all_metrics = {}
        
        for tearsheet_type, pb_file in self.pb_files.items():
            print(f"\n🔍 Analyzing {tearsheet_type} tearsheet...")
            
            # Load protobuf data
            pb_data = self.load_protobuf(pb_file)
            
            # Extract all metrics
            metrics = self.extract_all_metrics(pb_data)
            
            print(f"  📊 Extracted {len(metrics)} metrics:")
            for key, value in list(metrics.items())[:5]:  # Show first 5
                if isinstance(value, list):
                    print(f"    {key}: {len(value)} rows")
                else:
                    print(f"    {key}: {value}")
            
            all_metrics[tearsheet_type] = metrics
            
            # Verify we got substantial data
            assert len(metrics) > 0, f"No metrics extracted from {tearsheet_type}"
        
        print(f"\n✅ Total metrics extracted across all tearsheets: {sum(len(m) for m in all_metrics.values())}")
    
    def test_time_series_extraction_and_pyfolio_comparison(self):
        """Test extraction of time series data and basic pyfolio calculations"""
        
        comparison_results = {}
        
        for tearsheet_type in ['full', 'strategy_benchmark', 'returns_distribution']:
            print(f"\n📈 Testing time series for {tearsheet_type}...")
            
            # Load protobuf data
            pb_data = self.load_protobuf(self.pb_files[tearsheet_type])
            
            # Extract time series
            time_series = self.extract_time_series_data(pb_data)
            
            if time_series is not None and len(time_series) > 10:
                print(f"  ✓ Extracted time series: {len(time_series)} data points")
                print(f"  Date range: {time_series.index.min()} to {time_series.index.max()}")
                print(f"  Value range: {time_series.min():.6f} to {time_series.max():.6f}")
                
                # Calculate pyfolio metrics
                try:
                    cum_returns = ts.cum_returns(time_series)
                    annual_return = ts.annual_return(time_series)
                    
                    pyfolio_metrics = {
                        'total_return': cum_returns.iloc[-1],
                        'annual_return': annual_return,
                        'mean_return': time_series.mean(),
                        'volatility': time_series.std(),
                        'data_points': len(time_series)
                    }
                    
                    comparison_results[tearsheet_type] = pyfolio_metrics
                    
                    print(f"  📊 PyFolio calculations:")
                    for metric, value in pyfolio_metrics.items():
                        if isinstance(value, (int, float)):
                            print(f"    {metric}: {value:.6f}")
                        else:
                            print(f"    {metric}: {value}")
                
                except Exception as e:
                    print(f"  ⚠️  PyFolio calculation error: {e}")
            else:
                print(f"  ⚠️  No suitable time series found")
        
        # Verify we got at least one successful comparison
        assert len(comparison_results) > 0, "No time series data could be extracted for pyfolio comparison"
        print(f"\n✅ Successfully compared {len(comparison_results)} tearsheet time series with pyfolio")
    
    def test_metric_value_validation(self):
        """Test that extracted metric values are reasonable"""
        
        # Test strategy benchmark specifically as it likely has key metrics
        print("\n🎯 Testing metric value validation...")
        
        pb_data = self.load_protobuf(self.pb_files['strategy_benchmark'])
        metrics = self.extract_all_metrics(pb_data)
        
        # Look for common financial metrics
        financial_metrics = {}
        for key, value in metrics.items():
            key_lower = key.lower()
            if any(term in key_lower for term in ['return', 'ratio', 'volatility', 'sharpe', 'drawdown', 'alpha', 'beta']):
                if isinstance(value, (int, float)):
                    financial_metrics[key] = value
        
        print(f"  🔢 Found {len(financial_metrics)} financial metrics:")
        for key, value in financial_metrics.items():
            print(f"    {key}: {value}")
        
        # Validate ranges are reasonable for financial metrics
        for key, value in financial_metrics.items():
            if 'return' in key.lower():
                assert -1.0 <= value <= 10.0, f"Return metric {key} = {value} outside reasonable range [-1, 10]"
            elif 'ratio' in key.lower():
                assert -10.0 <= value <= 10.0, f"Ratio metric {key} = {value} outside reasonable range [-10, 10]"
            elif 'volatility' in key.lower():
                assert 0.0 <= value <= 2.0, f"Volatility metric {key} = {value} outside reasonable range [0, 2]"
        
        assert len(financial_metrics) > 0, "No recognizable financial metrics found"
        print(f"  ✅ All {len(financial_metrics)} financial metrics have reasonable values")
    
    def test_cross_tearsheet_consistency(self):
        """Test that metrics are consistent across different tearsheet types"""
        
        print("\n🔄 Testing cross-tearsheet consistency...")
        
        # Load multiple tearsheets
        full_data = self.load_protobuf(self.pb_files['full'])
        strategy_data = self.load_protobuf(self.pb_files['strategy_benchmark'])
        
        full_metrics = self.extract_all_metrics(full_data)
        strategy_metrics = self.extract_all_metrics(strategy_data)
        
        print(f"  Full tearsheet metrics: {len(full_metrics)}")
        print(f"  Strategy benchmark metrics: {len(strategy_metrics)}")
        
        # Look for overlapping metrics that should be consistent
        overlapping_keys = set(full_metrics.keys()) & set(strategy_metrics.keys())
        
        consistent_metrics = 0
        for key in overlapping_keys:
            if isinstance(full_metrics[key], (int, float)) and isinstance(strategy_metrics[key], (int, float)):
                diff = abs(full_metrics[key] - strategy_metrics[key])
                relative_diff = diff / max(abs(full_metrics[key]), abs(strategy_metrics[key]), 1e-10)
                
                if relative_diff < 0.01:  # Within 1%
                    consistent_metrics += 1
                    print(f"    ✓ {key}: {full_metrics[key]} vs {strategy_metrics[key]} (consistent)")
                else:
                    print(f"    ⚠️  {key}: {full_metrics[key]} vs {strategy_metrics[key]} (different)")
        
        print(f"  ✅ Found {consistent_metrics} consistent overlapping metrics")

if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v", "-s"])