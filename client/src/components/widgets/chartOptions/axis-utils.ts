import Highcharts from 'highcharts';
import { 
  StraightLine,
  Point,
  EpochFolioType,
  Scalar,
  AxisDef,
  AxisType
} from '../../../types';
import { formatValue } from '../../../utils/formatting';
import { getCubehelixPalette } from 'src/utils/colorPalettes';

// Define the missing interfaces we need
interface AxisDefinition {
  label?: string;
  type?: 'linear' | 'logarithmic' | 'datetime' | 'category';
  gridLineWidth?: number;
  min?: number;
  max?: number;
}

interface PlotBand {
  from: Scalar;
  to: Scalar;
  color?: string;
  label?: string;
}

interface Series {
  name?: string;
  type?: string;
  color?: string;
  dashStyle?: string;
  lineWidth?: number;
  data: Point[];
}

/**
 * Configures an axis based on the provided definition and options
 * @param axisDef The axis definition from chart data
 * @param options Additional options to apply
 * @returns Configured axis options
 */
export const configureAxis = (
  axisDef: AxisDefinition | undefined,
  options: Partial<Highcharts.XAxisOptions> | Partial<Highcharts.YAxisOptions> = {}
): Highcharts.XAxisOptions | Highcharts.YAxisOptions => {
  const axisOptions: Highcharts.XAxisOptions | Highcharts.YAxisOptions = {
    title: {
      text: axisDef?.label || null
    },
    gridLineWidth: axisDef?.gridLineWidth ?? 1,
    min: axisDef?.min,
    max: axisDef?.max,
    ...options
  };

  // Set axis type if specified
  if (axisDef?.type) {
    if (axisDef.type === 'linear' || axisDef.type === 'logarithmic' || 
        axisDef.type === 'datetime' || axisDef.type === 'category') {
      axisOptions.type = axisDef.type;
    }
  }

  return axisOptions;
};


/**
 * Configures an axis based on the provided definition and options
 * @param axisDef The axis definition from chart data
 * @param options Additional options to apply
 * @returns Configured axis options
 */
export const configureXAxis = (
  axisDef: AxisDefinition | undefined,
  options: Partial<Highcharts.XAxisOptions> = {}
): Highcharts.XAxisOptions => {
  return configureAxis(axisDef, options) as Highcharts.XAxisOptions;
};

/**
 * Configures a y-axis based on the provided definition and options
 * @param axisDef The axis definition from chart data
 * @param options Additional options to apply
 * @returns Configured axis options
 */
export const configureYAxis = (
  axisDef: AxisDefinition | undefined,
  options: Partial<Highcharts.YAxisOptions> = {}
): Highcharts.YAxisOptions => {
  return configureAxis(axisDef, options) as Highcharts.YAxisOptions;
};


/**
 * Processes plot bands for X-axis
 * @param plotBands Array of plot bands
 * @returns Array of Highcharts X-axis plot band options
 */
export const processAxisPlotBands = (
  plotBands: PlotBand[] | undefined
): Highcharts.XAxisPlotBandsOptions[] | Highcharts.YAxisPlotBandsOptions[] => {
  if (!plotBands || !Array.isArray(plotBands) || plotBands.length === 0) {
    return [];
  }

  const palette = getCubehelixPalette(plotBands.length, true);
  return plotBands.map((band, index) => ({
    from: band.from as number,
    to: band.to as number,
    color: band.color ?? Highcharts.color(palette[index]).setOpacity(0.4).get(),
  }));
};


/**
 * Processes plot bands for X-axis
 * @param plotBands Array of plot bands
 * @returns Array of Highcharts X-axis plot band options
 */
export const processXAxisPlotBands = (
  plotBands: PlotBand[] | undefined
): Highcharts.XAxisPlotBandsOptions[] => {
  return processAxisPlotBands(plotBands);
};

/**
 * Processes plot bands for Y-axis
 * @param plotBands Array of plot bands
 * @returns Array of Highcharts Y-axis plot band options
 */
export const processYAxisPlotBands = (
  plotBands: PlotBand[] | undefined
): Highcharts.YAxisPlotBandsOptions[] => {
  return processAxisPlotBands(plotBands);
};


/**
 * Processes straight lines for Y-axis
 * @param straightLines Array of straight lines
 * @returns Array of Highcharts Y-axis plot line options
 */
export const processAxisStraightLines = (
  straightLines: StraightLine[]
): Highcharts.XAxisPlotLinesOptions[] | Highcharts.YAxisPlotLinesOptions[] => {
  if (!straightLines || !Array.isArray(straightLines) || straightLines.length === 0) {
    return [];
  }

  return straightLines.map(line => ({
    value: line.value as number,
    color: '#FF0000',
    width: 1,
    dashStyle: 'Dash',
    zIndex: 1000,
    label: {
      text: line.title,
      style: {
        color: 'rgba(0, 0, 0, 0.6)',
        fontSize: '10px'
      }
    }
  }));
};

/**
 * Processes straight lines for X-axis
 * @param straightLines Array of straight lines
 * @returns Array of Highcharts X-axis plot line options
 */
export const processXAxisStraightLines = (
  straightLines: StraightLine[]
): Highcharts.XAxisPlotLinesOptions[] => {
  return processAxisStraightLines(straightLines);
};

/**
 * Processes straight lines for Y-axis
 * @param straightLines Array of straight lines
 * @returns Array of Highcharts Y-axis plot line options
 */
export const processYAxisStraightLines = (
  straightLines: StraightLine[]
): Highcharts.YAxisPlotLinesOptions[] => {
  return processAxisStraightLines(straightLines);
};


export const processAllAxisStraightLines = (
  straightLines: StraightLine[]
): [Highcharts.XAxisPlotLinesOptions[], Highcharts.YAxisPlotLinesOptions[]] => {
  return [processXAxisStraightLines(straightLines.filter((line: StraightLine) => !line.vertical)),
     processYAxisStraightLines(straightLines.filter((line: StraightLine) => line.vertical))];
};


/**
 * Formats a datetime value for Highcharts
 * @param value The datetime value to format
 * @returns Timestamp for Highcharts
 */
export const formatDatetimeValue = (value: string | number | Date): number => {
  if (value instanceof Date) {
    return value.getTime();
  }
  
  if (typeof value === 'number') {
    return value;
  }
  
  // Handle string dates
  try {
    return new Date(value).getTime();
  } catch (error) {
    console.error('Failed to parse date:', value, error);
    return 0;
  }
};

/**
 * Formats a chart point based on the x-axis type
 * @param point The data point
 * @param xAxisType The type of the x-axis ('datetime', 'category', or 'linear')
 * @returns Formatted point for Highcharts
 */
export const formatChartPoint = (
  point: Point, 
  xAxisType: 'datetime' | 'category' | 'linear'
): [number | string, number] | Highcharts.PointOptionsObject => {
  const xValue = point.x || '';
  const yValue = typeof point.y === 'number' ? point.y : parseFloat(point.y?.toString() || '0') || 0;
  
  if (xAxisType === 'datetime') {
    // For datetime, convert the x value to a timestamp
    const timestamp = formatDatetimeValue(xValue as string | number);
    return [timestamp, yValue];
  } else if (xAxisType === 'category') {
    // For category, use the x value as is
    return {
      name: xValue.toString(),
      y: yValue
    };
  } else {
    // For linear, parse the x value as a number if possible
    const numX = typeof xValue === 'number' ? xValue : parseFloat(xValue.toString()) || 0;
    return [numX, yValue];
  }
};

/**
 * Creates a series configuration for Highcharts
 * @param series The series data
 * @param xAxisType The type of the x-axis
 * @param index The index of the series
 * @param isHorizontal Whether the chart is horizontal (for bar charts)
 * @returns Highcharts series configuration
 */
export const createSeries = (
  series: Series,
  xAxisType: AxisType,
  index: number,
  isHorizontal: boolean = false
): Highcharts.SeriesOptionsType => {
  // Determine colors based on index if not provided
  const defaultColors = [
    '#2f7ed8', '#0d233a', '#8bbc21', '#910000', '#1aadce',
    '#492970', '#f28f43', '#77a1e5', '#c42525', '#a6c96a'
  ];

  const formattedData = series.data.map(point => 
    formatChartPoint(point, xAxisType as 'datetime' | 'category' | 'linear')
  );

  // Create the base series
  const result: Highcharts.SeriesOptionsType = {
    name: series.name || `Series ${index + 1}`,
    type: (series.type || 'line') as any,
    color: series.color || defaultColors[index % defaultColors.length],
    data: formattedData as any,
  };

  // Add optional properties if they exist
  if (series.dashStyle) {
    (result as any).dashStyle = series.dashStyle as Highcharts.DashStyleValue;
  }

  if (series.lineWidth) {
    (result as any).lineWidth = series.lineWidth;
  }

  // For horizontal charts, we might need to adjust data
  if (isHorizontal && result.type === 'column') {
    result.type = 'bar' as any;
  }

  return result;
}; 