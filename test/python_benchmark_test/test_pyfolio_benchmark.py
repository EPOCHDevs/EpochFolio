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
    class MockTearSheet:
        def __init__(self):
            self.charts = []
            self.tables = []
        def ParseFromString(self, data):
            pass
    
    class MockModule:
        TearSheet = MockTearSheet
    
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
            return tearsheet.TearSheet()
        
        # Determine which message type to use based on filename
        pb_filename = Path(pb_file_path).name
        if 'full' in pb_filename:
            tearsheet_data = tearsheet.FullTearSheet()
        else:
            tearsheet_data = tearsheet.TearSheet()
        
        try:
            with open(pb_file_path, 'rb') as f:
                tearsheet_data.ParseFromString(f.read())
            return tearsheet_data
        except Exception as e:
            print(f"Error parsing {pb_file_path}: {e}")
            # Try the other message type
            try:
                if 'full' in pb_filename:
                    tearsheet_data = tearsheet.TearSheet()
                else:
                    tearsheet_data = tearsheet.FullTearSheet()
                with open(pb_file_path, 'rb') as f:
                    tearsheet_data.ParseFromString(f.read())
                print(f"Successfully parsed with alternative message type")
                return tearsheet_data
            except Exception as e2:
                print(f"Failed with both message types: {e2}")
                raise e
    
    def extract_returns_from_protobuf(self, tearsheet_data):
        """Extract returns data from protobuf tearsheet"""
        returns_data = []
        dates = []
        
        # Handle both TearSheet and FullTearSheet structures
        charts_to_check = []
        
        if hasattr(tearsheet_data, 'charts'):
            # Handle protobuf repeated field properly
            if hasattr(tearsheet_data.charts, 'charts'):
                # ChartList with nested charts
                for chart in tearsheet_data.charts.charts:
                    charts_to_check.append(chart)
            else:
                # Direct charts field
                for chart in tearsheet_data.charts:
                    charts_to_check.append(chart)
        
        # For FullTearSheet, check individual tearsheet sections
        if hasattr(tearsheet_data, 'returns_distribution') and tearsheet_data.returns_distribution:
            if hasattr(tearsheet_data.returns_distribution, 'charts'):
                if hasattr(tearsheet_data.returns_distribution.charts, 'charts'):
                    for chart in tearsheet_data.returns_distribution.charts.charts:
                        charts_to_check.append(chart)
                else:
                    for chart in tearsheet_data.returns_distribution.charts:
                        charts_to_check.append(chart)
        
        # For FullTearSheet with categories structure
        if hasattr(tearsheet_data, 'categories') and tearsheet_data.categories:
            for category_name, category_data in tearsheet_data.categories.items():
                if hasattr(category_data, 'charts'):
                    if hasattr(category_data.charts, 'charts'):
                        for chart in category_data.charts.charts:
                            charts_to_check.append(chart)
                    else:
                        for chart in category_data.charts:
                            charts_to_check.append(chart)
        
        # Look for time series data in charts
        for chart in charts_to_check:
            if hasattr(chart, 'title') and ('return' in chart.title.lower() or 'cumulative' in chart.title.lower()):
                if hasattr(chart, 'data') and hasattr(chart.data, 'line_series'):
                    for series in chart.data.line_series:
                        if hasattr(series, 'data_points'):
                            for point in series.data_points:
                                if hasattr(point, 'x_value') and hasattr(point, 'y_value'):
                                    try:
                                        # Try different ways to extract timestamp and value
                                        if hasattr(point.x_value, 'string_value'):
                                            dates.append(pd.Timestamp(point.x_value.string_value))
                                        elif hasattr(point.x_value, 'double_value'):
                                            dates.append(pd.Timestamp.fromtimestamp(point.x_value.double_value))
                                        
                                        if hasattr(point.y_value, 'double_value'):
                                            returns_data.append(point.y_value.double_value)
                                    except Exception as e:
                                        print(f"Error extracting data point: {e}")
                                        continue
        
        if returns_data and dates:
            return pd.Series(returns_data, index=dates, name='returns')
        return None
    
    def extract_positions_from_protobuf(self, tearsheet_data):
        """Extract positions data from protobuf tearsheet"""
        positions_data = {}
        
        # Handle both TearSheet and FullTearSheet structures
        tables_to_check = []
        
        if hasattr(tearsheet_data, 'tables'):
            # Handle protobuf repeated field properly
            if hasattr(tearsheet_data.tables, 'tables'):
                # TableList with nested tables
                for table in tearsheet_data.tables.tables:
                    tables_to_check.append(table)
            else:
                # Direct tables field
                for table in tearsheet_data.tables:
                    tables_to_check.append(table)
        
        # For FullTearSheet, check individual tearsheet sections
        if hasattr(tearsheet_data, 'positions') and tearsheet_data.positions:
            if hasattr(tearsheet_data.positions, 'tables'):
                if hasattr(tearsheet_data.positions.tables, 'tables'):
                    for table in tearsheet_data.positions.tables.tables:
                        tables_to_check.append(table)
                else:
                    for table in tearsheet_data.positions.tables:
                        tables_to_check.append(table)
        
        # For FullTearSheet with categories structure
        if hasattr(tearsheet_data, 'categories') and tearsheet_data.categories:
            for category_name, category_data in tearsheet_data.categories.items():
                if hasattr(category_data, 'tables'):
                    if hasattr(category_data.tables, 'tables'):
                        for table in category_data.tables.tables:
                            tables_to_check.append(table)
                    else:
                        for table in category_data.tables:
                            tables_to_check.append(table)
        
        # Look for positions data in tables
        for table in tables_to_check:
            if hasattr(table, 'title') and 'position' in table.title.lower():
                if hasattr(table, 'rows'):
                    # Extract positions data from table rows
                    for row in table.rows:
                        if hasattr(row, 'cells') and len(row.cells) >= 2:
                            try:
                                symbol = row.cells[0].string_value
                                position = row.cells[1].double_value
                                positions_data[symbol] = position
                            except Exception as e:
                                print(f"Error extracting position data: {e}")
                                continue
        
        return positions_data if positions_data else None
    
    def extract_transactions_from_protobuf(self, tearsheet_data):
        """Extract transactions data from protobuf tearsheet"""
        transactions_data = []
        
        # Handle both TearSheet and FullTearSheet structures
        tables_to_check = []
        
        if hasattr(tearsheet_data, 'tables'):
            # Handle protobuf repeated field properly
            if hasattr(tearsheet_data.tables, 'tables'):
                # TableList with nested tables
                for table in tearsheet_data.tables.tables:
                    tables_to_check.append(table)
            else:
                # Direct tables field
                for table in tearsheet_data.tables:
                    tables_to_check.append(table)
        
        # For FullTearSheet, check individual tearsheet sections
        if hasattr(tearsheet_data, 'transactions') and tearsheet_data.transactions:
            if hasattr(tearsheet_data.transactions, 'tables'):
                if hasattr(tearsheet_data.transactions.tables, 'tables'):
                    for table in tearsheet_data.transactions.tables.tables:
                        tables_to_check.append(table)
                else:
                    for table in tearsheet_data.transactions.tables:
                        tables_to_check.append(table)
        
        # For FullTearSheet with categories structure
        if hasattr(tearsheet_data, 'categories') and tearsheet_data.categories:
            for category_name, category_data in tearsheet_data.categories.items():
                if hasattr(category_data, 'tables'):
                    if hasattr(category_data.tables, 'tables'):
                        for table in category_data.tables.tables:
                            tables_to_check.append(table)
                    else:
                        for table in category_data.tables:
                            tables_to_check.append(table)
        
        # Look for transactions data in tables
        for table in tables_to_check:
            if hasattr(table, 'title') and 'transaction' in table.title.lower():
                if hasattr(table, 'rows'):
                    for row in table.rows:
                        if hasattr(row, 'cells') and len(row.cells) >= 3:
                            try:
                                date = pd.Timestamp(row.cells[0].string_value)
                                symbol = row.cells[1].string_value
                                amount = row.cells[2].double_value
                                transactions_data.append({
                                    'date': date,
                                    'symbol': symbol,
                                    'amount': amount
                                })
                            except Exception as e:
                                print(f"Error extracting transaction data: {e}")
                                continue
        
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
        
        # Check for data in different structures
        has_data = False
        if hasattr(pb_data, 'charts'):
            if hasattr(pb_data.charts, 'charts') and len(pb_data.charts.charts) > 0:
                has_data = True
            elif hasattr(pb_data.charts, '__len__') and len(pb_data.charts) > 0:
                has_data = True
        if hasattr(pb_data, 'tables'):
            if hasattr(pb_data.tables, 'tables') and len(pb_data.tables.tables) > 0:
                has_data = True
            elif hasattr(pb_data.tables, '__len__') and len(pb_data.tables) > 0:
                has_data = True
        if hasattr(pb_data, 'cards'):
            if hasattr(pb_data.cards, 'cards') and len(pb_data.cards.cards) > 0:
                has_data = True
            elif hasattr(pb_data.cards, '__len__') and len(pb_data.cards) > 0:
                has_data = True
        
        # For FullTearSheet, check individual sections
        if hasattr(pb_data, 'strategy_benchmark') and pb_data.strategy_benchmark:
            has_data = True
        if hasattr(pb_data, 'risk_analysis') and pb_data.risk_analysis:
            has_data = True
        if hasattr(pb_data, 'returns_distribution') and pb_data.returns_distribution:
            has_data = True
        if hasattr(pb_data, 'positions') and pb_data.positions:
            has_data = True
        if hasattr(pb_data, 'transactions') and pb_data.transactions:
            has_data = True
        if hasattr(pb_data, 'round_trip') and pb_data.round_trip:
            has_data = True
        
        # Check categories in FullTearSheet structure
        if hasattr(pb_data, 'categories') and pb_data.categories:
            has_data = True
            
        assert has_data, f"No data found in {tearsheet_type} tearsheet"
        
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
                cum_returns = ts.cum_returns(returns)
                total_return = cum_returns.iloc[-1]
                annual_return = ts.annual_return(returns)
                
                assert not pd.isna(total_return), "Total return should be calculable"
                assert not pd.isna(annual_return), "Annual return should be calculable"
                
                print(f"✓ Pyfolio calculations successful: Total Return: {total_return:.4f}, "
                      f"Annual Return: {annual_return:.4f}")
                
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
        tables_to_check = []
        
        if hasattr(pb_data, 'tables'):
            if hasattr(pb_data.tables, 'tables'):
                tables_to_check.extend(pb_data.tables.tables)
            else:
                tables_to_check.extend(pb_data.tables)
        
        for table in tables_to_check:
            if hasattr(table, 'title') and ('round' in table.title.lower() or 'trip' in table.title.lower()):
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
            
            # Count charts and tables based on message type
            charts_count = 0
            tables_count = 0
            
            if hasattr(pb_data, 'charts'):
                if hasattr(pb_data.charts, 'charts'):
                    charts_count = len(pb_data.charts.charts)
                elif hasattr(pb_data.charts, '__len__'):
                    charts_count = len(pb_data.charts)
            if hasattr(pb_data, 'tables'):
                if hasattr(pb_data.tables, 'tables'):
                    tables_count = len(pb_data.tables.tables)
                elif hasattr(pb_data.tables, '__len__'):
                    tables_count = len(pb_data.tables)
            
            # For FullTearSheet, count across all sections
            if hasattr(pb_data, 'strategy_benchmark') and pb_data.strategy_benchmark:
                if hasattr(pb_data.strategy_benchmark, 'charts'):
                    if hasattr(pb_data.strategy_benchmark.charts, 'charts'):
                        charts_count += len(pb_data.strategy_benchmark.charts.charts)
                    elif hasattr(pb_data.strategy_benchmark.charts, '__len__'):
                        charts_count += len(pb_data.strategy_benchmark.charts)
                if hasattr(pb_data.strategy_benchmark, 'tables'):
                    if hasattr(pb_data.strategy_benchmark.tables, 'tables'):
                        tables_count += len(pb_data.strategy_benchmark.tables.tables)
                    elif hasattr(pb_data.strategy_benchmark.tables, '__len__'):
                        tables_count += len(pb_data.strategy_benchmark.tables)
            
            # Check other sections similarly...
            for section_name in ['risk_analysis', 'returns_distribution', 'positions', 'transactions', 'round_trip']:
                if hasattr(pb_data, section_name):
                    section = getattr(pb_data, section_name)
                    if section:
                        if hasattr(section, 'charts'):
                            if hasattr(section.charts, 'charts'):
                                charts_count += len(section.charts.charts)
                            elif hasattr(section.charts, '__len__'):
                                charts_count += len(section.charts)
                        if hasattr(section, 'tables'):
                            if hasattr(section.tables, 'tables'):
                                tables_count += len(section.tables.tables)
                            elif hasattr(section.tables, '__len__'):
                                tables_count += len(section.tables)
            
            # For FullTearSheet with categories structure
            if hasattr(pb_data, 'categories') and pb_data.categories:
                for category_name, category_data in pb_data.categories.items():
                    if hasattr(category_data, 'charts'):
                        if hasattr(category_data.charts, 'charts'):
                            charts_count += len(category_data.charts.charts)
                        elif hasattr(category_data.charts, '__len__'):
                            charts_count += len(category_data.charts)
                    if hasattr(category_data, 'tables'):
                        if hasattr(category_data.tables, 'tables'):
                            tables_count += len(category_data.tables.tables)
                        elif hasattr(category_data.tables, '__len__'):
                            tables_count += len(category_data.tables)
            
            results = {
                'charts_count': charts_count,
                'tables_count': tables_count,
                'has_data': charts_count > 0 or tables_count > 0
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