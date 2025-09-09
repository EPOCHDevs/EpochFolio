#!/usr/bin/env python3
"""
Complete benchmark runner for EpochFolio vs PyFolio comparison
"""

import subprocess
import sys
from pathlib import Path

def run_tests():
    """Run the complete benchmark test suite"""
    print("🚀 EpochFolio vs PyFolio Benchmark Suite")
    print("=" * 50)
    
    # Activate virtual environment and run tests
    result = subprocess.run([
        "bash", "-c", 
        "source venv/bin/activate && python -m pytest test_pyfolio_benchmark.py -v --tb=short"
    ], capture_output=True, text=True)
    
    print("📊 Test Results:")
    print(result.stdout)
    
    if result.stderr:
        print("⚠️ Warnings:")
        print(result.stderr)
    
    # Show summary
    if result.returncode == 0:
        print("✅ All tests passed!")
        print("\n📈 What was tested:")
        print("  • Protobuf data loading and parsing")
        print("  • Data extraction from 7 tearsheet types")
        print("  • PyFolio integration and calculations")
        print("  • Data structure consistency")
        print("  • Cross-validation between implementations")
        
        print("\n📋 Tearsheet Coverage:")
        tearsheet_types = [
            "full (39 charts, 10 tables)",
            "positions (7 charts, 3 tables)", 
            "returns_distribution (4 charts)",
            "risk_analysis (4 charts, 1 table)",
            "round_trip (6 charts, 5 tables)",
            "strategy_benchmark (14 charts, 1 table)",
            "transactions (4 charts)"
        ]
        for t in tearsheet_types:
            print(f"  • {t}")
        
        return True
    else:
        print("❌ Some tests failed")
        return False

def show_usage():
    """Show usage examples"""
    print("\n🔧 Usage Examples:")
    print("  # Run all tests:")
    print("  python run_benchmark.py")
    print("  ")
    print("  # Run specific test:")
    print("  source venv/bin/activate")
    print("  pytest test_pyfolio_benchmark.py::TestPyfolioBenchmark::test_full_tearsheet_comparison -v")
    print("  ")
    print("  # Run demo:")
    print("  source venv/bin/activate")
    print("  python example_benchmark_usage.py")

def main():
    """Main function"""
    try:
        success = run_tests()
        
        if success:
            show_usage()
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"💥 Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())