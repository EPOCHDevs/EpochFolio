import React, { useState, useEffect } from 'react';
import { Box, Paper, Typography } from '@mui/material';
import { DataGrid, GridColDef, GridCellParams } from '@mui/x-data-grid';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TextField,
} from '@mui/material';

interface DataTableWidgetProps {
  widget: {
    title?: string;
    type?: string;
    data?: any[];
    options?: any;
  };
}

const DataTableWidget: React.FC<DataTableWidgetProps> = ({ widget }) => {
  const { title, data } = widget;
  
  // Check if data is an array with headers and rows
  if (!Array.isArray(data) || data.length === 0) {
    return (
      <Paper sx={{ p: 2, height: '100%' }}>
        <Typography variant="h6" component="div" gutterBottom>
          {title}
        </Typography>
        <Typography>No data available</Typography>
      </Paper>
    );
  }

  // Handle case where data is an array of objects
  if (typeof data[0] === 'object' && data[0] !== null) {
    const headers = Object.keys(data[0]);
    
    return (
      <Paper sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
        <Typography variant="h6" component="div" gutterBottom>
          {title}
        </Typography>
        <TableContainer sx={{ flexGrow: 1, overflow: 'auto' }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                {headers.map((header) => (
                  <TableCell key={header}>{header}</TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {data.map((row, rowIndex) => (
                <TableRow key={rowIndex}>
                  {headers.map((header) => (
                    <TableCell key={`${rowIndex}-${header}`}>
                      {typeof row[header] === 'number' 
                        ? row[header].toFixed(4) 
                        : String(row[header] || '')}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    );
  }

  // Handle case where data is a simple 2D array with first row as headers
  const headers = data[0];
  
  return (
    <Paper sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h6" component="div" gutterBottom>
        {title}
      </Typography>
      <TableContainer sx={{ flexGrow: 1, overflow: 'auto' }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              {headers.map((header: any, index: number) => (
                <TableCell key={index}>{header}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {data.slice(1).map((row: any[], rowIndex: number) => (
              <TableRow key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <TableCell key={`${rowIndex}-${cellIndex}`}>
                    {typeof cell === 'number' ? cell.toFixed(4) : String(cell || '')}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Paper>
  );
};

export default DataTableWidget; 