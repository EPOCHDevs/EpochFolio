import Highcharts from 'highcharts';
import { PieChart, EpochFolioType, PieDataDef } from '../../../types';
import { formatValue } from '../../../utils/formatting';

/**
 * Generates Highcharts options for a pie chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing pie data
 * @returns Highcharts.Options object for pie chart
 */
export const getPieChartOptions = (
  baseOptions: Highcharts.Options,
  chartData: PieChart
): Highcharts.Options => {
  const chartDef = chartData.chartDef;
  const title = chartDef.title || 'Chart';

  // Generate series dynamically from the data array
  const series: Highcharts.SeriesPieOptions[] = [];

  if (Array.isArray(chartData.data) && chartData.data.length > 0) {
    const colors = Highcharts.getOptions().colors || [];

    // Create a series from each PieDataDef in the array
    chartData.data.forEach((serieDef: PieDataDef, index: number) => {
      const seriesData: Highcharts.PointOptionsObject[] = [];

      // Process the points for this series
      serieDef.points.forEach((point, pointIndex) => {
        // For the first series, use the standard colors
        // For inner series, apply brightness variations
        let pointColor = colors[pointIndex % colors.length];
        if (index > 0) {
          const brightness = 0.2 - (pointIndex / serieDef.points.length) / 5;
          pointColor = Highcharts.color(pointColor).brighten(brightness).get();
        }

        seriesData.push({
          name: point.name,
          y: point.y as number,
          color: pointColor
        });
      });

      // Build series configuration with all properties from PieDataDef
      const seriesOptions: Highcharts.SeriesPieOptions = {
        type: 'pie',
        name: serieDef.name,
        data: seriesData,
        size: serieDef.size,
        id: `series-${index}`
      };

      // Add innerSize if specified
      if (serieDef.innerSize) {
        seriesOptions.innerSize = serieDef.innerSize;

        // For inner rings (donuts), add specialized data labels
        seriesOptions.dataLabels = {
          format: '<b>{point.name}</b>: <span style="opacity: 0.5">{y}%</span>',
          filter: {
            property: 'y',
            operator: '>',
            value: 1
          },
          style: {
            fontWeight: 'normal'
          }
        };
      } else {
        // For outer rings, add contrasting data labels
        seriesOptions.dataLabels = {
          color: '#ffffff',
          distance: '-50%'
        };
      }

      series.push(seriesOptions);
    });
  }

  return {
    ...baseOptions,
    chart: {
      type: 'pie',
      height: 640
    },
    title: {
      text: title,
      align: 'left'
    },
    tooltip: {
      valueSuffix: '%'
    },
    series: series
  };
}; 