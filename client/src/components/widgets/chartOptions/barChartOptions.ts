import Highcharts from 'highcharts';
import { 
  BarChart,
  EpochFolioType,
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { 
  configureXAxis, 
  configureYAxis, 
  processAllAxisStraightLines
} from './axis-utils';


/**
 * Generates Highcharts options for a bar chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing bar data
 * @returns Highcharts.Options object for bar chart
 */
export const getBarChartOptions = (
  baseOptions: Highcharts.Options,
  chartData: BarChart,
  isHorizontal: boolean
): Highcharts.Options => {
  // Use the extended interface to avoid TypeScript errors
  const chartDef = chartData.chartDef;
  
  // Get axis definitions
  const xAxisDef = isHorizontal ? chartDef.yAxis : chartDef.xAxis;
  const yAxisDef = isHorizontal ? chartDef.xAxis : chartDef.yAxis;

  
  // Get straight lines if available
  const [xAxisStraightLines, yAxisStraightLines] = processAllAxisStraightLines(chartData.straightLines || []);


  // Create series from bar data or series
  const series : Highcharts.SeriesOptionsType[] = [{
    name: chartDef.title || 'Value',
    data: chartData.data as number[],
    type: isHorizontal ? 'bar' : 'column'
  }]

  // Configure x and y axes with proper options
  const xAxisOptions = configureXAxis(xAxisDef, {
    categories: xAxisDef?.categories,
    plotLines: xAxisStraightLines,
    title: {
      text: xAxisDef?.label || ''
    },
    labels: {
      style: {
        fontSize: '11px'
      },
      rotation: isHorizontal ? 0 : 270,
    }
  });

  const yAxisOptions = configureYAxis(yAxisDef, {
    categories: yAxisDef?.categories,
    plotLines: yAxisStraightLines,
    labels: {
      style: {
        fontSize: '11px'
      },
      // format: '{value}%'
    },
  });

  // Create the config object
  const config: Highcharts.Options = {
    ...baseOptions,
    yAxis: yAxisOptions,
    xAxis: xAxisOptions,
    title: {
      text: chartDef.title || 'Bar Chart',
      align: 'left'
    },
    chart: {
      type: isHorizontal ? 'bar' : 'column',
      height: 300
    },
    tooltip: {
      valueDecimals: 2,
      // formatter: function(this: any) {
      //   const value = formatValue(this.y, EpochFolioType.DECIMAL);
      //   return `<b>${this.key || this.x}</b>: ${value}`;
      // }
    },
    series
  };

  return config;
}; 