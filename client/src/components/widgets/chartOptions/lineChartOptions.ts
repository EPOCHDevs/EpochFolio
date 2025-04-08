import Highcharts from 'highcharts';
import { 
  EpochFolioType, 
  LineChart,
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { 
  configureXAxis,
  configureYAxis,
  processXAxisPlotBands, 
  processYAxisPlotBands,
  createSeries,
  processAllAxisStraightLines
} from './axis-utils';

/**
 * Generates Highcharts options for a line chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing lines
 * @returns Highcharts.Options object for line chart
 */
export const getLineChartOptions = (
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

  
  // Determine axis type
  const xAxisType = xAxisDef?.type ?? 'linear';
  const yAxisType = yAxisDef?.type ?? 'linear';
  
  // Create series from lines
  const series = (chartData.lines || []).map((line, index) => 
    createSeries(line, xAxisType, index)
  );
  
  // Add overlay line if it exists
  if (chartData.overlay) {
    const overlay = chartData.overlay;
    const overlaySeries = {
      ...createSeries(overlay, xAxisType, series.length),
      color: (overlay as any).color || '#ff7f0e', // Use a different color for overlay
      zIndex: 5, // Make sure overlay is above other lines
    };
    
    series.push(overlaySeries as Highcharts.SeriesOptionsType);
  }

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
    type: yAxisType,
    labels: {
      style: {
        fontSize: '11px'
      }
    }
  });
  
  // Add secondary y-axis if overlay exists
  // if (chartData.overlay) {
  //   yAxisOptions.push({
  //     title: {
  //       text: chartData.overlay.name || 'Overlay'
  //     },
  //     opposite: true,
  //     labels: {
  //       style: {
  //         fontSize: '11px'
  //       }
  //     }
  //   });
  // }

  return {
    ...baseOptions,
    title: {
      text: chartDef.title,
      align: 'left'
    },
    chart: {
      type: 'line',
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
    series: series as Highcharts.SeriesOptionsType[]
  };
}; 