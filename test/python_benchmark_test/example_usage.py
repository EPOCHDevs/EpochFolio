#!/usr/bin/env python3
"""
Example usage of epoch-protos package in local virtual environment
"""

import epoch_protos.common_pb2 as common
import epoch_protos.chart_def_pb2 as chart_def
import epoch_protos.table_def_pb2 as table_def
import epoch_protos.tearsheet_pb2 as tearsheet

def main():
    print("🚀 EpochProtos Python Package Example")
    print("=" * 50)
    
    # Example 1: Create a Scalar value
    scalar = common.Scalar()
    scalar.decimal_value = 123.45
    print(f"📊 Scalar value: {scalar.decimal_value}")
    
    # Example 2: Create a Chart Definition
    chart = chart_def.ChartDef()
    chart.id = "portfolio_performance"
    chart.title = "Portfolio Performance Over Time"
    chart.type = common.EPOCH_FOLIO_DASHBOARD_WIDGET_LINES
    print(f"📈 Chart: {chart.title} (ID: {chart.id})")
    
    # Example 3: Create a Table Definition
    table = table_def.Table()
    table.title = "Risk Metrics"
    table.type = common.EPOCH_FOLIO_DASHBOARD_WIDGET_DATA_TABLE
    print(f"📋 Table: {table.title}")
    
    # Example 4: Test serialization
    chart_data = chart.SerializeToString()
    print(f"💾 Chart serialized to {len(chart_data)} bytes")
    
    # Deserialize
    new_chart = chart_def.ChartDef()
    new_chart.ParseFromString(chart_data)
    print(f"🔄 Deserialized chart: {new_chart.title}")
    
    print("\n✅ All examples completed successfully!")
    print("🎉 epoch-protos is working correctly in your virtual environment!")

if __name__ == "__main__":
    main()
