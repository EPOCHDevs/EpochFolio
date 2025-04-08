import Highcharts from 'highcharts';
import { 
  EpochFolioType,
  HeatMapChart,
  HeatMapPoint
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { configureAxis } from './axis-utils';

/**
 * Generates Highcharts options for a heatmap chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing points
 * @returns Highcharts.Options object for heatmap
 */
export const getHeatMapOptions = (
  baseOptions: Highcharts.Options,
  chartData: HeatMapChart
): Highcharts.Options => {
  const chartDef = chartData.chartDef;
  const title = chartDef.title || 'Chart';
  // For heatmaps, use the points array from chartData
  const points = chartData.points || [];
  
  // Extract labels from x and y axis if available
  const xAxisDef = chartDef.xAxis;
  const yAxisDef = chartDef.yAxis;
  
  // Use custom labels if provided, otherwise use defaults
  const xLabels = xAxisDef?.categories ?? [];
  const yLabels = yAxisDef?.categories ?? [];
  
  
  // Configure x and y axes
  const xAxisOptions: Highcharts.XAxisOptions = configureAxis(xAxisDef, {
    categories: xLabels,
    title: {
      text: xAxisDef?.label || ''
    }
  }) as Highcharts.XAxisOptions;
  
  const yAxisOptions: Highcharts.YAxisOptions = configureAxis(yAxisDef, {
    categories: yLabels,
    reversed: true,
    title: {
      text: yAxisDef?.label || ''
    }
  }) as Highcharts.YAxisOptions;
  
  // Configure color axis
  const colorAxisOptions: Highcharts.ColorAxisOptions = {
    min: 0,
    minColor: '#FFFFFF',
    maxColor: Highcharts.getOptions().colors?.at(0) ?? '#000000'
  };
  
  return {
    ...baseOptions,
    chart: {
      type: 'heatmap',
      height: (chartData as any).height || 400
    },
    title: {
      text: title
    },
    legend: {
      align: 'right',
      layout: 'vertical',
      margin: 0,
      verticalAlign: 'top',
      y: 25,
      symbolHeight: 280
  },
    xAxis: xAxisOptions,
    yAxis: yAxisOptions,
    colorAxis: colorAxisOptions,
    tooltip: {
      formatter: function(this: any) {
        // Format the correlation value with formatValue
        const formattedValue = formatValue(this.point.value, EpochFolioType.DECIMAL);
        return '<b>' + 
          (yLabels[this.point.y] || '') + ' / ' + 
          (xLabels[this.point.x] || '') + 
          '</b><br>Value: ' + 
          formattedValue;
      }
    },
    series: [{
      name: title,
      borderWidth: 1,
      data: chartData.points,
      dataLabels: {
        enabled: (chartData as any).showDataLabels !== false,
        color: '#000000',
        format: '{point.value:.1f}',
        style: {
          fontSize: '10px',
          textOutline: 'none'
        }
      }
    }] as Highcharts.SeriesOptionsType[]
  };
}; 