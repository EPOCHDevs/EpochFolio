#!/usr/bin/env python3
"""
Setup and run the pyfolio benchmark tests
"""

import subprocess
import sys
from pathlib import Path

def setup_environment():
    """Setup the test environment"""
    print("🚀 Setting up test environment...")
    
    # Create virtual environment if it doesn't exist
    venv_path = Path("test_venv")
    if not venv_path.exists():
        print("📦 Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", "test_venv"], check=True)
    
    # Get pip and python paths
    if sys.platform == "win32":
        pip_path = venv_path / "Scripts" / "pip"
        python_path = venv_path / "Scripts" / "python"
    else:
        pip_path = venv_path / "bin" / "pip"
        python_path = venv_path / "bin" / "python"
    
    # Install requirements
    print("📥 Installing requirements...")
    subprocess.run([str(pip_path), "install", "-r", "requirements.txt"], check=True)
    subprocess.run([str(pip_path), "install", "-e", "pyfolio-reloaded/"], check=True)
    subprocess.run([str(pip_path), "install", "epoch-protos"], check=True)
    
    return python_path

def run_tests(python_path):
    """Run the benchmark tests"""
    print("🧪 Running benchmark tests...")
    
    # Run pytest with verbose output
    result = subprocess.run([
        str(python_path), "-m", "pytest", 
        "test_pyfolio_benchmark.py", 
        "-v", 
        "--tb=short"
    ], capture_output=True, text=True)
    
    print("📊 Test Results:")
    print(result.stdout)
    if result.stderr:
        print("⚠️ Warnings/Errors:")
        print(result.stderr)
    
    return result.returncode == 0

def main():
    """Main function"""
    try:
        python_path = setup_environment()
        success = run_tests(python_path)
        
        if success:
            print("✅ All tests completed successfully!")
        else:
            print("❌ Some tests failed. Check output above.")
            return 1
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())