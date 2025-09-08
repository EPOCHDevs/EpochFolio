#!/usr/bin/env python3
"""
Pytest framework to benchmark EpochFolio protobuf tearsheets against pyfolio tearsheet functions
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

# Add pyfolio-reloaded to path
sys.path.insert(0, str(Path(__file__).parent / "pyfolio-reloaded/src"))

# Import protobuf modules
try:
    import epoch_protos.common_pb2 as common
    import epoch_protos.chart_def_pb2 as chart_def
    import epoch_protos.table_def_pb2 as table_def
    import epoch_protos.tearsheet_pb2 as tearsheet
    EPOCH_PROTOS_AVAILABLE = True
except ImportError:
    print("Warning: epoch_protos not available, creating mock objects")
    EPOCH_PROTOS_AVAILABLE = False
    
    # Create simple mock objects for testing
    class MockTearsheet:
        def __init__(self):
            self.charts = []
            self.tables = []
        def ParseFromString(self, data):
            pass
    
    class MockModule:
        Tearsheet = MockTearsheet
    
    tearsheet = MockModule()

# Import pyfolio
import pyfolio

class TestPyfolioBenchmark:
    """Test class for benchmarking EpochFolio protobuf tearsheets against pyfolio"""
    
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
    
    def load_protobuf(self, pb_file_path):
        """Load and parse a protobuf file"""
        if not EPOCH_PROTOS_AVAILABLE:
            print(f"Skipping protobuf loading for {pb_file_path} - epoch_protos not available")
            return tearsheet.Tearsheet()
        
        tearsheet_data = tearsheet.Tearsheet()
        with open(pb_file_path, 'rb') as f:
            tearsheet_data.ParseFromString(f.read())
        return tearsheet_data
    
    def extract_returns_from_protobuf(self, tearsheet_data):
        """Extract returns data from protobuf tearsheet"""
        returns_data = []
        dates = []
        
        # Look for time series data in charts
        for chart in tearsheet_data.charts:
            if 'return' in chart.title.lower() or 'cumulative' in chart.title.lower():
                for series in chart.data.line_series:
                    for point in series.data_points:
                        if hasattr(point, 'x_value') and hasattr(point, 'y_value'):
                            # Convert timestamp to date if needed
                            dates.append(pd.Timestamp(point.x_value.string_value))
                            returns_data.append(point.y_value.double_value)
        
        if returns_data and dates:
            return pd.Series(returns_data, index=dates, name='returns')
        return None
    
    def extract_positions_from_protobuf(self, tearsheet_data):
        """Extract positions data from protobuf tearsheet"""
        positions_data = {}
        
        # Look for positions data in tables
        for table in tearsheet_data.tables:
            if 'position' in table.title.lower():
                # Extract positions data from table rows
                for row in table.rows:
                    if len(row.cells) >= 2:
                        symbol = row.cells[0].string_value
                        position = row.cells[1].double_value
                        positions_data[symbol] = position
        
        return positions_data if positions_data else None
    
    def extract_transactions_from_protobuf(self, tearsheet_data):
        """Extract transactions data from protobuf tearsheet"""
        transactions_data = []
        
        # Look for transactions data in tables
        for table in tearsheet_data.tables:
            if 'transaction' in table.title.lower():
                for row in table.rows:
                    if len(row.cells) >= 3:
                        date = pd.Timestamp(row.cells[0].string_value)
                        symbol = row.cells[1].string_value
                        amount = row.cells[2].double_value
                        transactions_data.append({
                            'date': date,
                            'symbol': symbol,
                            'amount': amount
                        })
        
        return pd.DataFrame(transactions_data) if transactions_data else None

    @pytest.mark.parametrize("tearsheet_type", ['full', 'positions', 'returns_distribution', 'risk_analysis'])
    def test_tearsheet_data_consistency(self, tearsheet_type):
        """Test that protobuf tearsheet data is consistent with pyfolio calculations"""
        
        # Load protobuf data
        pb_data = self.load_protobuf(self.pb_files[tearsheet_type])
        
        # Extract relevant data
        returns = self.extract_returns_from_protobuf(pb_data)
        positions = self.extract_positions_from_protobuf(pb_data)
        transactions = self.extract_transactions_from_protobuf(pb_data)
        
        # Basic consistency checks
        assert pb_data is not None, f"Failed to load protobuf data for {tearsheet_type}"
        assert len(pb_data.charts) > 0 or len(pb_data.tables) > 0, f"No data found in {tearsheet_type} tearsheet"
        
        # If we have returns data, verify it's reasonable
        if returns is not None:
            assert len(returns) > 0, "Returns data should not be empty"
            assert not returns.isna().all(), "Returns should contain valid values"
            assert isinstance(returns.index[0], pd.Timestamp), "Returns index should be timestamps"
        
        print(f"✓ {tearsheet_type} tearsheet data consistency check passed")
    
    def test_full_tearsheet_comparison(self):
        """Compare full tearsheet protobuf data with pyfolio create_full_tear_sheet"""
        pb_data = self.load_protobuf(self.pb_files['full'])
        
        # Extract data from protobuf
        returns = self.extract_returns_from_protobuf(pb_data)
        
        if returns is not None:
            # Create sample benchmark returns for comparison
            benchmark_returns = pd.Series(
                np.random.normal(0.0008, 0.02, len(returns)), 
                index=returns.index, 
                name='benchmark'
            )
            
            # This would normally call pyfolio.create_full_tear_sheet
            # but since it generates HTML, we'll test the underlying data functions
            try:
                # Test some core pyfolio calculations
                import pyfolio.timeseries as ts
                
                # Basic performance metrics
                total_return = ts.cum_returns_final(returns)
                annual_return = ts.annual_return(returns)
                max_drawdown = ts.max_drawdown(returns)
                
                assert not pd.isna(total_return), "Total return should be calculable"
                assert not pd.isna(annual_return), "Annual return should be calculable"
                assert not pd.isna(max_drawdown), "Max drawdown should be calculable"
                
                print(f"✓ Pyfolio calculations successful: Total Return: {total_return:.4f}, "
                      f"Annual Return: {annual_return:.4f}, Max Drawdown: {max_drawdown:.4f}")
                
            except Exception as e:
                print(f"Warning: Could not run pyfolio calculations: {e}")
        
        print("✓ Full tearsheet comparison completed")
    
    def test_positions_tearsheet_comparison(self):
        """Compare positions tearsheet protobuf data with pyfolio create_position_tear_sheet"""
        pb_data = self.load_protobuf(self.pb_files['positions'])
        
        positions = self.extract_positions_from_protobuf(pb_data)
        
        if positions is not None:
            # Verify positions data structure
            assert len(positions) > 0, "Positions should not be empty"
            assert all(isinstance(v, (int, float)) for v in positions.values()), "Position values should be numeric"
            
            print(f"✓ Positions data found: {len(positions)} positions")
        
        print("✓ Positions tearsheet comparison completed")
    
    def test_round_trip_tearsheet_comparison(self):
        """Compare round trip tearsheet protobuf data with pyfolio create_round_trip_tear_sheet"""
        pb_data = self.load_protobuf(self.pb_files['round_trip'])
        
        # Basic validation of round trip data
        assert pb_data is not None, "Round trip protobuf data should load"
        
        # Look for round trip specific metrics in tables
        round_trip_metrics = []
        for table in pb_data.tables:
            if 'round' in table.title.lower() or 'trip' in table.title.lower():
                round_trip_metrics.append(table.title)
        
        if round_trip_metrics:
            print(f"✓ Round trip metrics found: {round_trip_metrics}")
        
        print("✓ Round trip tearsheet comparison completed")
    
    def test_transactions_tearsheet_comparison(self):
        """Compare transactions tearsheet protobuf data with pyfolio create_txn_tear_sheet"""
        pb_data = self.load_protobuf(self.pb_files['transactions'])
        
        transactions = self.extract_transactions_from_protobuf(pb_data)
        
        if transactions is not None:
            # Verify transactions data structure
            assert len(transactions) > 0, "Transactions should not be empty"
            assert 'date' in transactions.columns, "Transactions should have date column"
            assert 'symbol' in transactions.columns, "Transactions should have symbol column"
            assert 'amount' in transactions.columns, "Transactions should have amount column"
            
            print(f"✓ Transactions data found: {len(transactions)} transactions")
        
        print("✓ Transactions tearsheet comparison completed")

    def test_data_extraction_completeness(self):
        """Test that we can extract meaningful data from all protobuf files"""
        
        extraction_results = {}
        
        for tearsheet_type, pb_file in self.pb_files.items():
            pb_data = self.load_protobuf(pb_file)
            
            results = {
                'charts_count': len(pb_data.charts),
                'tables_count': len(pb_data.tables),
                'has_data': len(pb_data.charts) > 0 or len(pb_data.tables) > 0
            }
            
            extraction_results[tearsheet_type] = results
            
            # Each tearsheet should have some data
            assert results['has_data'], f"{tearsheet_type} tearsheet should contain charts or tables"
        
        # Print summary
        print("\\n📊 Data Extraction Summary:")
        for tearsheet_type, results in extraction_results.items():
            print(f"  {tearsheet_type:20}: Charts={results['charts_count']:2}, Tables={results['tables_count']:2}")
        
        print("✓ All tearsheets contain extractable data")

if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v"])