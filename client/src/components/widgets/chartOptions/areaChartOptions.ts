import Highcharts from 'highcharts';
import { 
  LineChart, 
  EpochFolioType,
  StraightLine
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { 
  configureXAxis, 
  configureYAxis, 
  processXAxisPlotBands, 
  processYAxisPlotBands,
  processAllAxisStraightLines,
  createSeries 
} from './axis-utils';

/**
 * Generates Highcharts options for an area chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing lines (will be rendered as areas)
 * @returns Highcharts.Options object for area chart
 */
export const getAreaChartOptions = (
  baseOptions: Highcharts.Options,
  chartData: LineChart
): Highcharts.Options => {
  const chartDef = chartData.chartDef;
  
  // Get axis definitions
  const xAxisDef = chartDef.xAxis;
  const yAxisDef = chartDef.yAxis;
  
  // Get plot bands if available
  const xPlotBands = processXAxisPlotBands(chartData.xPlotBands);
  const yPlotBands = processYAxisPlotBands(chartData.yPlotBands);
  
  // Get straight lines if available
  const [xAxisStraightLines, yAxisStraightLines] = processAllAxisStraightLines(chartData.straightLines || []);
  
  // Check if chart should be stacked
  const isStacked = chartData.stacked === true;
  const xAxisType = xAxisDef?.type ?? 'linear';
  
  // Create series from lines but make them areas
  const series = (chartData.lines || []).map((line: any, index: number) => {
    const seriesOptions = createSeries(line, xAxisType, index);
    return {
      ...seriesOptions,
      type: 'area',
      fillOpacity: line.fillOpacity !== undefined ? line.fillOpacity : 0.3
    };
  });

  // Configure x and y axes with proper options
  const xAxisOptions = configureXAxis(xAxisDef, {
    plotBands: xPlotBands,
    plotLines: xAxisStraightLines,
    type: xAxisType,
    labels: {
      style: {
        fontSize: '11px'
      },
      // Explicitly set rotation to 0 to avoid undefined rotation issue
      rotation: 0,
      overflow: 'allow'
    }
  });

  const yAxisOptions = configureYAxis(yAxisDef, {
    plotBands: yPlotBands,
    plotLines: yAxisStraightLines,
    labels: {
      style: {
        fontSize: '11px'
      }
    }
  });

  return {
    ...baseOptions,
    title: {
      text: chartDef.title,
      align: 'left'
    },
    chart: {
      type: 'area',
      height: 300
    },
    xAxis: xAxisOptions,
    yAxis: yAxisOptions,
    tooltip: {
      valueDecimals: 2,
      formatter: function(this: any) {
        if (xAxisType === 'datetime') {
          const date = new Date(this.x);
          const formattedDate = formatValue(date.toISOString(), EpochFolioType.DATETIME);
          // Format each y value with appropriate formatting
          const pointsStr = this.points?.map((point: any) => {
            const formattedValue = formatValue(point.y, EpochFolioType.DECIMAL);
            return `<span style="color:${point.color}">\u25CF</span> ${point.series.name}: <b>${formattedValue}</b>`;
          }).join('<br/>') || '';
          return `<b>${formattedDate}</b><br/>${pointsStr}`;
        } else {
          return this.defaultFormatter.call(this);
        }
      }
    },
    plotOptions: {
      area: {
        stacking: isStacked ? 'normal' : undefined,
        marker: {
          enabled: false
        }
      }
    },
    series: series as Highcharts.SeriesOptionsType[]
  };
}; 