# EpochFolio

[![C++23](https://img.shields.io/badge/C++-23-blue.svg)](https://isocpp.org/)
[![CMake](https://img.shields.io/badge/CMake-3.26+-blue.svg)](https://cmake.org/)
[![React](https://img.shields.io/badge/React-19.x-blue.svg)](https://reactjs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-4.9+-blue.svg)](https://www.typescriptlang.org/)

EpochFolio is a comprehensive portfolio analysis and visualization toolkit consisting of a high-performance C++ backend library and a modern React TypeScript frontend dashboard. It provides powerful tools for quantitative finance, portfolio performance analysis, risk metrics calculation, and interactive data visualization.

## 🏗️ Architecture

```
EpochFolio/
├── 📊 C++ Backend Library     # High-performance financial analysis
│   ├── Portfolio tearsheet generation
│   ├── Risk metrics & performance analytics  
│   ├── Returns distribution analysis
│   └── JSON data export
│
└── 🖥️ React Frontend Dashboard # Interactive visualization
    ├── Multi-chart visualization (Highcharts)
    ├── Data tables & summary cards
    ├── Responsive 3-column layout
    └── Category-based organization
```

## ✨ Key Features

### Backend Library (C++)
- **Portfolio Performance**: Comprehensive performance metrics and analysis
- **Risk Analysis**: Advanced risk metrics, drawdown analysis, and volatility measures
- **Returns Distribution**: Statistical analysis of returns with rolling statistics
- **Position Tracking**: Position sizing, allocation analysis, and exposure tracking
- **Transaction Analysis**: Trade analysis with cost and slippage metrics
- **Round Trip Analysis**: Complete trade cycle analysis from entry to exit
- **High Performance**: Built on Apache Arrow for efficient data processing
- **JSON Export**: Structured tearsheet data export for frontend consumption

### Frontend Dashboard (React + TypeScript)
- **Multiple Chart Types**: Line, Area, Bar, Histogram, Box Plot, Heat Map, Pie, X-Range
- **Interactive Tables**: Sortable, filterable data grids with Material-UI
- **Summary Cards**: Key metrics display in card format
- **Responsive Design**: 3-column adaptive layout
- **Category Organization**: Organized tabs for different analysis types
- **File Upload**: Drag-and-drop tearsheet JSON file support

## 🛠️ Technology Stack

### Backend Dependencies
| Dependency | Purpose | Features |
|------------|---------|----------|
| **Arrow** | Data processing | Acero, Dataset, CSV, JSON, Parquet, S3, Filesystem |
| **EpochFrame** | Financial data framework | Core data structures and utilities |
| **Glaze** | JSON serialization | High-performance JSON handling |
| **TBB** | Parallel processing | Threading Building Blocks |
| **SPDLog** | Logging | Fast, header-only logging |
| **Catch2** | Testing framework | Modern C++ testing (v3+) |
| **Trompeloeil** | Mocking framework | C++ mocking library (v47+) |
| **Tabulate** | Table formatting | Console table output |

### Frontend Dependencies
- **React 19.x** - Modern React with concurrent features
- **TypeScript 4.9+** - Type-safe JavaScript
- **Material-UI** - Modern component library
- **Highcharts** - Professional charting library
- **Recharts** - Additional charting components

## 🚀 Quick Start

### Prerequisites
- **C++ Compiler**: GCC 11+, Clang 14+, or MSVC 2022+
- **CMake**: 3.26 or higher
- **vcpkg**: Package manager for C++ dependencies
- **Node.js**: 16+ for frontend development

### Backend Setup

1. **Configure vcpkg** (if not already done):
```bash
# Set vcpkg toolchain path
export VCPKG_ROOT="$HOME/vcpkg"
```

2. **Build the library**:
```bash
# Clone and build
git clone <repository-url>
cd EpochFolio

# Create build directory
mkdir build && cd build

# Configure with CMake
cmake .. -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake

# Build
make -j$(nproc)
```

3. **Build with tests**:
```bash
cmake .. -DBUILD_TEST=ON -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
make -j$(nproc)

# Run tests
ctest --output-on-failure
```

### Frontend Setup [Currently Broken]

1. **Install dependencies**:
```bash
cd client
npm install
```

2. **Start development server**:
```bash
npm start
```

The dashboard will be available at http://localhost:3000

## 📁 Project Structure

```
EpochFolio/
├── 📄 CMakeLists.txt              # Main build configuration
├── 📄 vcpkg.json                  # C++ dependency manifest
├── 📁 cmake/                      # CMake modules
│   └── EpochFrame.cmake           # EpochFrame dependency
├── 📁 include/epoch_folio/        # Public headers
│   ├── tearsheet.h                # Main tearsheet factory
│   ├── metadata.h                 # Widget & category definitions
│   ├── aliases.h                  # Type aliases
│   └── empyrical_all.h           # Statistical functions
├── 📁 src/                        # Implementation
│   ├── tearsheet.cpp             # Main tearsheet implementation
│   ├── empyrical/                # Statistical analysis
│   ├── portfolio/                # Portfolio models
│   └── tear_sheets/              # Analysis modules
│       ├── returns/              # Returns analysis
│       ├── positions/            # Position analysis
│       ├── transactions/         # Transaction analysis
│       └── round_trip/           # Round trip analysis
├── 📁 test/                       # Test suite
│   ├── tearsheet_test.cpp        # Main tearsheet tests
│   ├── empyrical/                # Statistical tests
│   └── portfolio/                # Portfolio tests
└── 📁 client/                     # React frontend
    ├── package.json              # Frontend dependencies
    ├── src/                      # React components
    └── public/                   # Static assets
```

## 🔧 Build Options

### CMake Configuration Options

```bash
# Shared libraries (default)
cmake .. -DBUILD_SHARED_LIBS=ON

# Static libraries
cmake .. -DBUILD_SHARED_LIBS=OFF

# Enable testing
cmake .. -DBUILD_TEST=ON

# Enable code coverage
cmake .. -DBUILD_TEST=ON -DENABLE_COVERAGE=ON

# Build examples
cmake .. -DBUILD_EXAMPLES=ON
```

### Build Targets

```bash
# Build main library
make epoch_folio

# Build and run tests
make epoch_folio_test
./bin/epoch_folio_test

# Build specific test targets
make epoch_folio_empyrical_test
make epoch_folio_portfolio_test
```

## 🧪 Testing

The project uses **Catch2 v3** for testing and **Trompeloeil** for mocking:

```bash
# Run all tests
cd build
ctest --output-on-failure

# Run specific test suite
./bin/epoch_folio_test [tag]

# Run with coverage (requires ENABLE_COVERAGE=ON)
make coverage
```

### Test Structure
- `test/tearsheet_test.cpp` - Main tearsheet functionality
- `test/empyrical/` - Statistical analysis tests  
- `test/portfolio/` - Portfolio model tests
- `test/common_utils.h` - Shared test utilities

## 📊 Data Format

### Tearsheet JSON Structure
```json
{
  "title": "Portfolio Performance Tearsheet",
  "categories": [
    {
      "name": "Strategy & Benchmark",
      "widgets": [
        {
          "type": "Lines",
          "title": "Cumulative Returns",
          "data": [...],
          "options": {...}
        }
      ]
    }
  ]
}
```

### Supported Widget Types
| Type | Description | Use Case |
|------|-------------|----------|
| `Card` | Key-value pairs | Summary metrics |
| `DataTable` | Tabular data | Detailed listings |
| `Lines` | Line chart | Time series |
| `Area` | Area chart | Cumulative data |
| `HeatMap` | Heat map | Correlation matrices |
| `Bar`/`Column` | Bar charts | Categorical data |
| `Histogram` | Distribution | Returns distribution |
| `BoxPlot` | Box plots | Statistical summaries |
| `XRange` | Range chart | Drawdown periods |
| `Pie` | Pie chart | Allocation breakdown |

## 🎯 Usage Example

### C++ Backend
```cpp
#include <epoch_folio/tearsheet.h>

using namespace epoch_folio;

// Configure tearsheet options
TearSheetDataOption options{
    .returns_path = "s3://bucket/returns.csv",
    .positions_path = "s3://bucket/positions.csv",
    .transactions_path = "s3://bucket/transactions.csv",
    .benchmark_path = "s3://bucket/benchmark.parquet"
};

// Create tearsheet factory
PortfolioTearSheetFactory factory(options);

// Generate tearsheet
TearSheetOption tearsheet_opts{};
auto tearsheet = factory.MakeTearSheet(tearsheet_opts);

// Export to JSON for frontend
auto json_output = serialize_tearsheet(tearsheet);
```

### Frontend Integration
```typescript
// Upload and visualize tearsheet
const handleFileUpload = async (file: File) => {
  const jsonData = await file.text();
  const tearsheet = JSON.parse(jsonData);
  setTearsheetData(tearsheet);
};
```

## 🤝 Contributing

1. Follow C++23 standards and modern best practices
2. Ensure all tests pass before submitting PRs
3. Use Catch2 for new tests, Trompeloeil for mocking
4. Follow the existing code style and formatting
5. Update documentation for new features

## 📝 License

[Add your license information here]

## 🔗 Dependencies & Credits

- **EpochFrame**: Core financial data framework
- **Apache Arrow**: High-performance data processing
- **Highcharts**: Professional charting library
- **Material-UI**: Modern React components

---

**Built with ❤️ for quantitative finance and portfolio analysis**
