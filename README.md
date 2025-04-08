# EpochFolio

EpochFolio is a comprehensive portfolio analysis and visualization toolkit consisting of a C++ backend library and a React TypeScript frontend dashboard. It provides powerful tools for quantitative finance, portfolio performance analysis, risk metrics calculation, and visualization of financial data.

## Project Overview

EpochFolio consists of two main components:

1. **C++ Backend Library**: A high-performance financial analysis library built on Arrow for data processing and analysis
2. **React Frontend Dashboard**: A modern web interface for visualizing portfolio tearsheet data with interactive charts and tables

## Backend Library

The C++ backend library provides:

- Portfolio performance metrics and analysis
- Risk analysis tools
- Returns distribution analysis
- Position tracking and analysis
- Transaction analysis
- Round trip trade analysis
- JSON export of tearsheet data

### Dependencies

- Arrow (with Acero, Dataset, CSV, JSON, Parquet, S3, and Filesystem features)
- Glaze (for JSON serialization)
- TBB (Threading Building Blocks)
- SPDLog (for logging)
- Catch2 and Trompeloeil (for testing)
- Tabulate (for table formatting)

### Building

The library uses CMake for building:

```bash
mkdir build && cd build
cmake ..
make
```

## Frontend Dashboard

The React frontend provides:

- Upload tearsheet JSON files
- Visualize data using various chart types (using Highcharts)
- Display data tables and cards
- Organized view with tabs for different analysis categories
- Responsive 3-column layout

### Features

- **Multiple Visualization Types**: Line charts, area charts, heat maps, bar charts, histograms, box plots, x-range charts, pie charts
- **Data Tables**: Interactive tables for detailed data exploration
- **Summary Cards**: At-a-glance metrics in card format
- **Category Organization**: Data organized by analysis category (Strategy & Benchmark, Risk Analysis, Returns Distribution, Positions, Transactions, Round Trip)

### Setup

1. Install dependencies:

```bash
cd client
npm install
```

2. Start the development server:

```bash
npm start
```

The application will be available at http://localhost:3000

## Data Structure

The application uses a structured tearsheet format organized into categories:

- **Strategy & Benchmark**: Performance comparisons between portfolio and benchmark
- **Risk Analysis**: Risk metrics and drawdown analysis
- **Returns Distribution**: Statistical analysis of returns
- **Positions**: Position sizing and allocation
- **Transactions**: Trade analysis
- **Round Trip**: Complete trade cycle analysis

## Sample Data

A sample JSON file is provided at `/client/public/sample_tearsheet.json` for testing purposes.

## License

[License Information]
