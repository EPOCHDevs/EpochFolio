# PyFolio Benchmark Test Suite

This test suite compares EpochFolio protobuf tearsheet data with pyfolio tearsheet functions to ensure data consistency and correctness.

## Setup

1. **Install dependencies**:
   ```bash
   python3 -m venv test_venv
   source test_venv/bin/activate
   pip install -r requirements.txt
   pip install -e pyfolio-reloaded/
   ```

2. **Install epoch-protos package**:
   You need to install or make available the epoch_protos Python package. This package should contain the protobuf-generated Python files for:
   - `epoch_protos.common_pb2`
   - `epoch_protos.chart_def_pb2`
   - `epoch_protos.table_def_pb2`
   - `epoch_protos.tearsheet_pb2`

## Test Structure

### Available Tests

1. **test_tearsheet_data_consistency**: Tests each tearsheet type for basic data consistency
2. **test_full_tearsheet_comparison**: Compares full tearsheet with pyfolio calculations
3. **test_positions_tearsheet_comparison**: Validates positions data structure
4. **test_round_trip_tearsheet_comparison**: Checks round trip analytics
5. **test_transactions_tearsheet_comparison**: Validates transaction data
6. **test_data_extraction_completeness**: Ensures all tearsheets contain extractable data

### Tearsheet Types Tested

- `full`: Full tearsheet analysis
- `positions`: Portfolio positions analysis
- `returns_distribution`: Returns distribution analysis
- `risk_analysis`: Risk metrics analysis
- `round_trip`: Round trip analysis
- `strategy_benchmark`: Strategy vs benchmark comparison
- `transactions`: Transaction analysis

## Running Tests

```bash
# Activate virtual environment
source test_venv/bin/activate

# Run all tests
pytest test_pyfolio_benchmark.py -v

# Run specific test
pytest test_pyfolio_benchmark.py::TestPyfolioBenchmark::test_full_tearsheet_comparison -v

# Run tests for specific tearsheet type
pytest test_pyfolio_benchmark.py::TestPyfolioBenchmark::test_tearsheet_data_consistency[full] -v
```

## Test Data

The tests expect protobuf files to be available at:
```
/home/adesola/EpochLab/EpochFolio/cmake-build-debug/bin/test_output/
├── full_test_result.pb
├── positions_test_result.pb
├── returns_distribution_test_result.pb
├── risk_analysis_test_result.pb
├── round_trip_test_result.pb
├── strategy_benchmark_test_result.pb
└── transactions_test_result.pb
```

## What the Tests Do

### Data Extraction
The tests extract data from protobuf tearsheets including:
- **Returns data**: Time series of portfolio returns
- **Positions data**: Portfolio holdings by symbol
- **Transactions data**: Trade history

### Pyfolio Integration
The tests validate that:
- Extracted data can be used with pyfolio functions
- Basic performance metrics can be calculated
- Data structures are compatible with pyfolio expectations

### Comparisons
The framework is designed to:
- Load protobuf tearsheet data
- Extract relevant time series and tabular data
- Compare with pyfolio function outputs
- Validate data consistency and correctness

## Current Status

✅ **Working**:
- Pytest framework setup
- Pyfolio integration and imports
- Test structure and organization
- Mock objects for testing without epoch_protos

⚠️ **Needs Setup**:
- epoch_protos package installation
- Actual protobuf data parsing and extraction
- Full data comparison implementation

## Next Steps

1. **Install epoch_protos**: Make the Python protobuf package available
2. **Validate protobuf structure**: Ensure the protobuf files match expected schema
3. **Enhance data extraction**: Improve methods to extract data from complex protobuf structures
4. **Add more comparisons**: Compare specific metrics between EpochFolio and pyfolio outputs
5. **Performance benchmarking**: Add timing comparisons between implementations

## Example Usage

```python
# Load protobuf tearsheet
pb_data = test_instance.load_protobuf('full_test_result.pb')

# Extract returns data
returns = test_instance.extract_returns_from_protobuf(pb_data)

# Use with pyfolio
import pyfolio.timeseries as ts
total_return = ts.cum_returns_final(returns)
annual_return = ts.annual_return(returns)
max_drawdown = ts.max_drawdown(returns)

# Compare with protobuf metrics
assert abs(total_return - pb_data.summary.total_return) < 0.001
```

## Contributing

To extend the test suite:
1. Add new test methods to `TestPyfolioBenchmark` class
2. Implement data extraction methods for new tearsheet types
3. Add validation logic for specific metrics
4. Update this README with new test descriptions