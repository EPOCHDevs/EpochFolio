export enum EpochFolioDashboardWidget {
  CARD = 'Card',
  DATA_TABLE = 'DataTable',
  LINES = 'Lines',
  AREA = 'Area',
  HEAT_MAP = 'HeatMap',
  BAR = 'Bar',
  COLUMN = 'Column',
  HISTOGRAM = 'Histogram',
  BOX_PLOT = 'BoxPlot',
  X_RANGE = 'XRange',
  PIE = 'Pie'
}

export enum EpochFolioType {
  PERCENT = 'Percent',
  DECIMAL = 'Decimal',
  INTEGER = 'Integer',
  DATE = 'Date',
  DATETIME = 'DateTime',
  MONETARY = 'Monetary',
  DURATION = 'Duration',
  STRING = 'String',
  DAY_DURATION = 'DayDuration'
}

export enum EpochFolioCategory {
  STRATEGY_BENCHMARK = 'StrategyBenchmark',
  RISK_ANALYSIS = 'RiskAnalysis',
  RETURNS_DISTRIBUTION = 'ReturnsDistribution',
  POSITIONS = 'Positions',
  TRANSACTIONS = 'Transactions',
  ROUND_TRIP = 'RoundTrip'
}


export const kLinearAxisType = 'linear';
export const kLogarithmicAxisType = 'logarithmic';
export const kDateTimeAxisType = 'datetime';
export const kCategoryAxisType = 'category';

export type AxisType = 'linear' | 'logarithmic' | 'datetime' | 'category';
// --- Axis Definitions ---
export interface AxisDef {
  type?: AxisType;
  label?: string;
  categories: string[];
}

// --- Chart Definitions ---
export interface ChartDef {
  id: string;
  title: string;
  type: EpochFolioDashboardWidget;
  category: EpochFolioCategory;
  yAxis?: AxisDef;
  xAxis?: AxisDef;
}

export type Scalar = number | string | boolean;

// --- Point Definitions ---
export interface Point {
  x: Scalar;
  y: Scalar;
}
export type Points = Point[];

export type HeatMapPoint = [Scalar, Scalar, Scalar];

export type HeatMapPoints = HeatMapPoint[];

export interface XRange {
  chartDef: ChartDef;
  categories: string[];
  points: XRangePoint[];
}

export interface PieData {
  name: string;
  data: Scalar[];
  categories: string[];
}

// --- Line Definitions ---
export interface Line {
  data: Points;
  name: string;
  dashStyle?: string;
  lineWidth?: number;
}
export type SeriesLines = Line[];

export interface StraightLine {
  title: string;
  value: Scalar;
  vertical: boolean;
}
export type StraightLines = StraightLine[];

export interface Band {
  from: Scalar;
  to: Scalar;
}

// --- Chart Types ---
export interface LineChart {
  chartDef: ChartDef;
  lines: SeriesLines;
  straightLines: StraightLines;
  yPlotBands: Band[];
  xPlotBands: Band[];
  overlay?: Line;
  stacked: boolean;
}

export interface HeatMapChart {
  chartDef: ChartDef;
  points: HeatMapPoints;
}

export interface XRangePoint {
  x: Scalar;
  x2: Scalar;
  y: number;
  is_long: boolean;
}

export interface XRangeChart {
  chartDef: ChartDef;
  categories: string[];
  points: XRangePoint[];
}

export interface BarChart {
  chartDef: ChartDef;
  data: Scalar[];
  straightLines: StraightLines;
  barWidth?: number;
}

export interface PieData {
  name: string;
  y: Scalar;
}

export type PieDataPoints = PieData[];

export interface PieDataDef {
  name: string;
  points: PieDataPoints;
  size: string;
  innerSize?: string;
}

export interface PieChart {
  chartDef: ChartDef;
  data: PieDataDef[];
}

export interface HistogramChart {
  chartDef: ChartDef;
  data: Scalar[];
  straightLines: StraightLines;
  binsCount?: number;
}

export interface BoxPlotDataPoint {
  low: number;
  q1: number;
  median: number;
  q3: number;
  high: number;
}

export type BoxPlotOutliers = [number, number][];

export type BoxPlotDataPoints = BoxPlotDataPoint[];

export interface BoxPlotDataPointDef {
  outliers: BoxPlotOutliers;
  points: BoxPlotDataPoints;
}

export interface BoxPlot {
  chartDef: ChartDef;
  data: BoxPlotDataPointDef;
}

export type Chart = LineChart | HeatMapChart | BarChart | HistogramChart | BoxPlot | XRangeChart | PieChart;

// --- Table Definitions ---
export interface ColumnDef {
  id: string;
  name: string;
  type: EpochFolioType;
}
export type ColumnDefs = ColumnDef[];

export interface Table {
  type: EpochFolioDashboardWidget
  category: EpochFolioCategory;
  title: string;
  columns: ColumnDefs;
  data: Record<string, Scalar>[];
}

// --- Card Definitions ---
export interface CardData {
  title: string;
  value: Scalar;
  type?: EpochFolioType;
  group?: number;
}

export interface Card {
  title: EpochFolioDashboardWidget;
  category: EpochFolioCategory;
  data: CardData[];
  group_size?: number;
}

// Main tearsheet data interface

export interface Tearsheet {
  cards: Card[];
  charts: Chart[];
  tables: Table[];
}
export interface FullTearsheet {
  strategy_benchmark: Tearsheet;
  risk_analysis: Tearsheet;
  returns_distribution: Tearsheet;
  positions: Tearsheet;
  transactions: Tearsheet;
  round_trip: Tearsheet;
} 