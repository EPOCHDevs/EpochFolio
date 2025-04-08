import React, { useRef, useEffect, useState } from 'react';
import { Typography, Box, Paper } from '@mui/material';
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
// Load Highcharts modules
import { Chart, EpochFolioDashboardWidget, LineChart, BarChart, HeatMapChart, PieChart, HistogramChart, BoxPlot, XRangeChart } from '../../types';
// Import chart options from separate files
import {
  getLineChartOptions,
  getAreaChartOptions,
  getBarChartOptions,
  getHistogramChartOptions,
  getPieChartOptions,
  getHeatMapOptions,
  getBoxPlotOptions
} from './chartOptions';
import highchartsHeatmap from "highcharts/modules/heatmap"; // The Heatmap module
import highchartsHistogram from "highcharts/modules/histogram-bellcurve";
import highchartsMore from "highcharts/highcharts-more"; // Contains boxplot functionality
import highchartsXrange from "highcharts/modules/xrange"; // Contains xrange functionality
import { getXRangeOptions } from './chartOptions/xRangeOptions';

if (typeof highchartsHeatmap === 'function') {
  highchartsHeatmap(Highcharts);
}

if (typeof highchartsHistogram === 'function') {
  highchartsHistogram(Highcharts);
}

if (typeof highchartsXrange === 'function') {
  highchartsXrange(Highcharts);
}

// Initialize highcharts-more which includes boxplot
if (typeof highchartsMore === 'function') {
  highchartsMore(Highcharts);
}


// Custom colors
const COLORS = [
  '#2f7ed8', '#0d233a', '#8bbc21', '#910000', '#1aadce',
  '#492970', '#f28f43', '#77a1e5', '#c42525', '#a6c96a'
];

interface ChartRendererProps {
  chartData: Chart;
}

// Base Highcharts options to be extended by specific chart types
const baseOptions: Highcharts.Options = {
  colors: COLORS,
  credits: {
    enabled: false
  },
  exporting: {
    enabled: true
  },
  legend: {
    enabled: true
  },
  accessibility: {
    enabled: true
  }
};

// Helper function to check if a chart has chartDef property
const hasChartDef = (chart: any): chart is { chartDef: { type: EpochFolioDashboardWidget } } => {
  return chart && chart.chartDef && typeof chart.chartDef.type !== 'undefined';
};

const ChartRenderer: React.FC<ChartRendererProps> = ({ chartData }) => {
  const [options, setOptions] = useState<Highcharts.Options>(baseOptions);

  useEffect(() => {
    if (!chartData) return;

    try {
      // Check if chartData has chartDef (BoxPlot doesn't)
      if (!hasChartDef(chartData)) {
        console.warn('Chart data does not have a chartDef property');
        setOptions({
          ...baseOptions,
          title: {
            text: 'Unsupported chart format',
            align: 'left'
          }
        });
        return;
      }

      const chartDef = chartData.chartDef;
      const chartType = chartDef.type;

      // Using type assertions to handle different chart types safely
      switch (chartType) {
        case EpochFolioDashboardWidget.LINES:
          setOptions(getLineChartOptions(baseOptions, chartData as LineChart));
          break;
        case EpochFolioDashboardWidget.AREA:
          setOptions(getAreaChartOptions(baseOptions, chartData as LineChart));
          break;
        case EpochFolioDashboardWidget.BAR:
          setOptions(getBarChartOptions(baseOptions, chartData as BarChart, true));
          break;
        case EpochFolioDashboardWidget.COLUMN:
          setOptions(getBarChartOptions(baseOptions, chartData as BarChart, false));
          break;
        case EpochFolioDashboardWidget.HISTOGRAM:
          setOptions(getHistogramChartOptions(baseOptions, chartData as HistogramChart));
          break;
        case EpochFolioDashboardWidget.PIE:
          setOptions(getPieChartOptions(baseOptions, chartData as PieChart));
          break;
        case EpochFolioDashboardWidget.HEAT_MAP:
          setOptions(getHeatMapOptions(baseOptions, chartData as HeatMapChart));
          break;
        case EpochFolioDashboardWidget.BOX_PLOT:
          setOptions(getBoxPlotOptions(baseOptions, chartData as BoxPlot));
          break;
        case EpochFolioDashboardWidget.X_RANGE:
          setOptions(getXRangeOptions(baseOptions, chartData as XRangeChart));
          break;
        default:
          console.warn(`Unsupported chart type: ${chartType}`);
          setOptions({
            ...baseOptions,
            title: {
              text: 'Unsupported chart type'
            }
          });
      }
    } catch (error) {
      console.error('Error rendering chart:', error);
      setOptions({
        ...baseOptions,
        title: {
          text: 'Chart could not be rendered'
        }
      });
    }
  }, [chartData]);

  if (!chartData) {
    return (
      <Paper sx={{ p: 3, height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography variant="subtitle1" color="text.secondary">
          No chart data available
        </Typography>
      </Paper>
    );
  }

  return (
    <Box sx={{ height: '100%', p: 0 }}>
      <HighchartsReact
        highcharts={Highcharts}
        options={options}
      />
    </Box>
  );
};

export default ChartRenderer; 