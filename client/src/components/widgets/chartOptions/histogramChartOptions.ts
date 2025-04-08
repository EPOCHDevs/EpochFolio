import Highcharts from 'highcharts';
import { 
  BarChart,
  EpochFolioType,
  StraightLine,
  Scalar,
  HistogramChart
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { 
  configureXAxis, 
  configureYAxis, 
  processAllAxisStraightLines,
} from './axis-utils';
import { XAxis } from 'recharts';


/**
 * Generates Highcharts options for a bar chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing bar data
 * @returns Highcharts.Options object for bar chart
 */
export const getHistogramChartOptions = (
  baseOptions: Highcharts.Options,
  chartData: HistogramChart
): Highcharts.Options => {
  // Use the extended interface to avoid TypeScript errors
  const chartDef = chartData.chartDef;
  
  // Get axis definitions
  const xAxisDef = chartDef.xAxis;
  const yAxisDef = chartDef.yAxis;
  
  // Get straight lines if available
  const [xAxisStraightLines, yAxisStraightLines] = processAllAxisStraightLines(chartData.straightLines || []);


  // Create series from bar data or series
  const series : Highcharts.SeriesOptionsType[] = [{
    name: xAxisDef?.label || '',
    type: 'histogram',
    baseSeries: 's1',
    zIndex: -1
}, {
    name: 'Data',
    type: 'scatter',
    data: chartData.data as number[],
    id: 's1',
    showInLegend: false,
    visible: false
}]

  // Configure x and y axes with proper options
  const xAxisOptions = configureXAxis(xAxisDef, {
    plotLines: xAxisStraightLines,
    categories: xAxisDef?.categories,
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
    plotLines: yAxisStraightLines,
    labels: {
      style: {
        fontSize: '11px'
      },
      rotation: 0,
      overflow: 'allow'
    }
  });

  // Create the config object
  const config: Highcharts.Options = {
    ...baseOptions,
    yAxis: yAxisOptions,
    xAxis: xAxisOptions,
    title: {
      text: chartDef.title,
      align: 'left'
    },
    chart: {
      height: 300
    },
    tooltip: {
      valueDecimals: 2,
      // formatter: function(this: any) {
      //   const value = formatValue(this.y, EpochFolioType.DECIMAL);
      //   return `<b>${this.key || this.x}</b>: ${value}`;
      // }
    },
    plotOptions: {
      histogram: {
        binsNumber: chartData.binsCount,
      }
    },
    series
  };

  return config;
}; 