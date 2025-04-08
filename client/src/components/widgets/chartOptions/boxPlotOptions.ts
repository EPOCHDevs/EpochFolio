import Highcharts from 'highcharts';
import { 
  EpochFolioType,
  BoxPlot,
  BoxPlotDataPointDef,
  BoxPlotDataPoint
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { 
  configureXAxis, 
  configureYAxis, 
  processAllAxisStraightLines
} from './axis-utils';
import { getCubehelixPalette } from '../../../utils/colorPalettes';

interface Outlier {
  x: number;
  y: number;
}

/**
 * Generates Highcharts options for a box plot chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing box plot data
 * @returns Highcharts.Options object for box plot chart
 */
export const getBoxPlotOptions = (
  baseOptions: Highcharts.Options,
  chartData: BoxPlot
): Highcharts.Options => {
  const chartDef = chartData.chartDef;
  
  // Get axis definitions
  const xAxisDef = chartDef.xAxis;
  const yAxisDef = chartDef.yAxis;
  
  // Transform the data to Highcharts format
  // BoxPlot data is in format [low, q1, median, q3, high]
  const boxPlotData = chartData.data;

  // Define the main series
  const mainSeries: Highcharts.SeriesOptionsType = {
    name: chartDef.title || 'Box Plot',
    type: 'boxplot',
    data: boxPlotData.points,
    tooltip: {
      headerFormat: '<em>{point.key}</em><br/>',
      pointFormat: 'Minimum: {point.low}<br/>' +
                  'Lower quartile: {point.q1}<br/>' +
                  'Median: {point.median}<br/>' +
                  'Upper quartile: {point.q3}<br/>' +
                  'Maximum: {point.high}<br/>'
    }
  };

  const outlierSeries: Highcharts.SeriesOptionsType = {
    name: 'Outliers',
    type: 'scatter',
    data: boxPlotData.outliers,
    color: Highcharts.getOptions().colors?.[0] || '#000000',
    marker: {
      fillColor: 'white',
      lineWidth: 1,
      lineColor: Highcharts.getOptions().colors?.[0] || '#000000'
    },
    tooltip: {
      pointFormat: 'Outlier: {point.y}'
    }
  }
  
  // Create outlier series if we have outliers
  let series: Highcharts.SeriesOptionsType[] = [mainSeries, outlierSeries];
  
  // Configure x and y axes with proper options
  const xAxisOptions = configureXAxis(xAxisDef, {
    categories: xAxisDef?.categories || [],
    title: {
      text: xAxisDef?.label || ''
    },
    labels: {
      style: {
        fontSize: '11px'
      }
    }
  });

  const yAxisOptions = configureYAxis(yAxisDef, {
    title: {
      text: yAxisDef?.label || 'Values'
    },
    labels: {
      style: {
        fontSize: '11px'
      }
    }
  });

  return {
    ...baseOptions,
    title: {
      text: chartDef.title || 'Box Plot',
      align: 'left'
    },
    chart: {
      type: 'boxplot',
      height: 300
    },
    legend: {
      enabled: true
    },
    xAxis: xAxisOptions,
    yAxis: yAxisOptions,
    plotOptions: {
      boxplot: {
        fillColor: Highcharts.getOptions().colors?.[0] || '#f0f0f0',
        lineWidth: 1,
        medianWidth: 1,
        stemWidth: 1,
        whiskerWidth: 2,
        whiskerLength: '50%',
        whiskerColor: '#333333'
      }
    },
    series
  };
}; 