import Highcharts from 'highcharts';
import { XRangeChart, XRangePoint } from '../../../types';

/**
 * Generates Highcharts options for an X-Range chart
 * @param baseOptions Base chart options to extend
 * @param chartData The chart data containing x-range data
 * @returns Highcharts.Options object for x-range chart
 */
export const getXRangeOptions = (
    baseOptions: Highcharts.Options,
    chartData: XRangeChart
): Highcharts.Options => {
    const chartDef = chartData.chartDef;
    const title = chartDef.title || 'Chart';
    const categories = chartData.categories || [];

    // Transform the data points with color logic based on is_long flag
    const transformedData = chartData.points.map((point: XRangePoint) => {
        return {
            type: 'xrange',
            x: point.x as string,
            x2: point.x2 as string,
            y: point.y,
            color: point.is_long ? '#2f7ed8' : '#d62c20', // Blue for long, red for short
        };
    }) as Highcharts.SeriesXrangeOptions[];

    return {
        ...baseOptions,
        chart: {
            type: 'xrange',
            height: 400
        },
        title: {
            text: title,
            align: 'left'
        },
        xAxis: {
            type: 'datetime'
        },
        yAxis: {
            title: {
                text: ''
            },
            categories: categories,
            reversed: true
        },
        series: [{
            name: 'RoundTrip Lifetime',
            type: 'xrange',
            borderColor: 'gray',
            pointWidth: 20,
            data: transformedData as Highcharts.SeriesXrangeOptions[],
            dataLabels: {
                enabled: true
            }
        }] as Highcharts.SeriesOptionsType[]
    };
}; 