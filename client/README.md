# EpochFolio Dashboard

A React TypeScript application for visualizing portfolio tearsheet data.

## Features

- Upload tearsheet JSON files
- Visualize data using various chart types (using Highcharts)
- Display data tables and cards
- Organized view with tabs and a responsive 3-column layout

## Setup

1. Install dependencies:

```bash
npm install
```

2. Start the development server:

```bash
npm start
```

The application will be available at http://localhost:3000

## JSON File Format

The application expects a JSON file with the following structure:

```json
{
  "title": "Portfolio Performance Tearsheet",
  "categories": [
    {
      "name": "Category Name",
      "widgets": [
        {
          "type": "Widget Type",
          "title": "Widget Title",
          "data": [...],
          "options": {...}
        }
      ]
    }
  ]
}
```

## Supported Widget Types

- Card: Display key-value pairs in a card format
- DataTable: Display tabular data
- Lines: Line chart
- Area: Area chart
- HeatMap: Heat map chart
- HorizontalBar: Horizontal bar chart
- Histogram: Histogram chart
- BoxPlots: Box plot chart
- XRange: X-range chart
- Pie: Pie chart

## Sample Data

A sample JSON file is provided at `/public/sample_tearsheet.json` for testing purposes.
