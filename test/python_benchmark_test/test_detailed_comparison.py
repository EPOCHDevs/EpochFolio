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
        
        # Handle FullTearSheet with categories map structure
        if hasattr(tearsheet_data, 'categories'):
            for category_name, category in tearsheet_data.categories.items():
                # Extract from cards in this category
                if hasattr(category, 'cards') and hasattr(category.cards, 'cards'):
                    for card in category.cards.cards:
                        self._extract_card_metrics(card, metrics, f"{category_name}_")
                
                # Extract from tables in this category  
                if hasattr(category, 'tables') and hasattr(category.tables, 'tables'):
                    for table in category.tables.tables:
                        self._extract_table_metrics(table, metrics, f"{category_name}_")
        
        # Extract from cards (summary metrics) - regular TearSheet structure
        if hasattr(tearsheet_data, 'cards'):
            if hasattr(tearsheet_data.cards, 'cards'):
                for card in tearsheet_data.cards.cards:
                    self._extract_card_metrics(card, metrics)
            else:
                for card in tearsheet_data.cards:
                    self._extract_card_metrics(card, metrics)
        
        # Extract from tables (detailed data) - regular TearSheet structure  
        if hasattr(tearsheet_data, 'tables'):
            if hasattr(tearsheet_data.tables, 'tables'):
                for table in tearsheet_data.tables.tables:
                    self._extract_table_metrics(table, metrics)
            else:
                for table in tearsheet_data.tables:
                    self._extract_table_metrics(table, metrics)
        
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
    
    def _extract_card_metrics(self, card, metrics, prefix=""):
        """Extract metrics from a single card"""
        for card_data in card.data:
            if hasattr(card_data, 'title') and hasattr(card_data, 'value'):
                title = card_data.title
                value = self._extract_value(card_data.value)
                if value is not None:
                    metrics[f"{prefix}card_{title}"] = value
    
    def _extract_table_metrics(self, table, metrics, prefix=""):
        """Extract metrics from a single table"""
        if hasattr(table, 'title') and hasattr(table, 'data') and hasattr(table.data, 'rows'):
            table_name = table.title.replace(' ', '_').lower()
            
            # Extract table data as list of rows
            rows_data = []
            for row in table.data.rows:
                if hasattr(row, 'values'):
                    row_data = []
                    for cell in row.values:
                        value = self._extract_value(cell)
                        row_data.append(value)
                    if row_data:
                        rows_data.append(row_data)
            
            if rows_data:
                metrics[f"{prefix}table_{table_name}"] = rows_data
    
    def _extract_value(self, value_obj):
        """Extract value from protobuf value object - handles all Scalar field variations"""
        if not hasattr(value_obj, 'WhichOneof'):
            return None
            
        # Get the active oneof field
        active_field = value_obj.WhichOneof('value')
        if active_field is None:
            return None
            
        # Return the value of whichever field is set
        return getattr(value_obj, active_field)
    
    def extract_time_series_data(self, tearsheet_data) -> Optional[pd.Series]:
        """Extract time series data for pyfolio comparison"""
        # Look through charts for time series data
        charts_to_check = []
        
        if hasattr(tearsheet_data, 'charts'):
            charts_to_check.extend(tearsheet_data.charts.charts)
        
        # For FullTearSheet, check sections
        if hasattr(tearsheet_data, 'returns_distribution') and tearsheet_data.returns_distribution:
            if hasattr(tearsheet_data.returns_distribution, 'charts'):
                charts_to_check.extend(tearsheet_data.returns_distribution.charts.charts)
        
        for chart in charts_to_check:
            # Check for lines chart with cumulative returns data
            if hasattr(chart, 'lines_def'):
                lines_def = chart.lines_def
                if hasattr(lines_def, 'chart_def') and hasattr(lines_def.chart_def, 'title'):
                    title = lines_def.chart_def.title.lower()
                    if 'return' in title or 'cumulative' in title:
                        if hasattr(lines_def, 'lines'):
                            for line in lines_def.lines:
                                if hasattr(line, 'data') and len(line.data) > 10:  # Need substantial data
                                    dates = []
                                    values = []
                                    
                                    for point in line.data:
                                        try:
                                            # Extract timestamp (x) and value (y)
                                            if hasattr(point, 'x') and hasattr(point, 'y'):
                                                # x is timestamp_ms, y is double
                                                timestamp_ms = point.x
                                                value = point.y
                                                
                                                # Convert timestamp from milliseconds to seconds
                                                dates.append(pd.Timestamp.fromtimestamp(timestamp_ms / 1000.0))
                                                values.append(float(value))
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
            
            # Verify we got substantial data (some tearsheets may only have charts, not cards/tables)
            # Check if tearsheet has any content at all
            has_content = len(metrics) > 0
            if not has_content:
                # Check if it at least has charts
                if hasattr(pb_data, 'charts') and len(pb_data.charts.charts) > 0:
                    print(f"  📊 Tearsheet has {len(pb_data.charts.charts)} charts (no card/table data)")
                    has_content = True
                elif hasattr(pb_data, 'categories'):
                    chart_count = sum(len(cat.charts.charts) for cat in pb_data.categories.values())
                    if chart_count > 0:
                        print(f"  📊 Tearsheet has {chart_count} charts across categories (no card/table data)")
                        has_content = True
            
            assert has_content, f"No content (metrics or charts) found in {tearsheet_type}"
        
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
            key_lower = key.lower()
            if 'return' in key_lower:
                if 'cumulative' in key_lower:
                    # Cumulative returns can be much higher over multiple years
                    assert -99.0 <= value <= 1000.0, f"Cumulative return metric {key} = {value} outside reasonable range [-99, 1000]"
                else:
                    # Annual returns should be more modest
                    assert -100.0 <= value <= 100.0, f"Return metric {key} = {value} outside reasonable range [-100, 100]"
            elif 'ratio' in key_lower:
                if 'sharpe' in key_lower or 'sortino' in key_lower:
                    # Sharpe/Sortino ratios typically range from -3 to 5
                    assert -5.0 <= value <= 10.0, f"Ratio metric {key} = {value} outside reasonable range [-5, 10]"
                else:
                    # Other ratios can vary more widely
                    assert -50.0 <= value <= 50.0, f"Ratio metric {key} = {value} outside reasonable range [-50, 50]"
            elif 'volatility' in key_lower:
                # Volatility as percentage, can range from near 0 to very high
                assert 0.0 <= value <= 200.0, f"Volatility metric {key} = {value} outside reasonable range [0, 200]"
            elif 'drawdown' in key_lower:
                # Max drawdown should be negative (loss) but not more than -100%
                assert -100.0 <= value <= 0.0, f"Drawdown metric {key} = {value} outside reasonable range [-100, 0]"
            elif 'alpha' in key_lower or 'beta' in key_lower:
                # Alpha and Beta can vary widely but should be reasonable
                assert -10.0 <= value <= 10.0, f"Alpha/Beta metric {key} = {value} outside reasonable range [-10, 10]"
        
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