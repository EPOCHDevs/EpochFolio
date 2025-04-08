import React, { useEffect } from 'react';
import { Table as TableType, EpochFolioType } from '../../types';
import { Paper, Typography, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material';
import { formatValue } from '../../utils/formatting';

interface TableRendererProps {
  tableData: TableType;
}

const TableRenderer: React.FC<TableRendererProps> = ({ tableData }) => {
  useEffect(() => {
    // Debug log to check data structure
    console.log('Table data received:', tableData);
  }, [tableData]);

  if (!tableData) {
    return (
      <Paper elevation={2} sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
        <Typography variant="body1">No table data available</Typography>
      </Paper>
    );
  }

  // Check for both old and new data structures
  let tableTitle = '';
  let tableColumns: any[] = [];
  let tableRows: any[] = [];

  // New data structure format
  if (tableData.type === 'DataTable') {
    console.log('Processing DataTable format');
    // Use type assertion to access properties safely
    // @ts-ignore - Safely access properties that might not be defined on the type
    tableTitle = tableData.category || 'Data Table';
    // @ts-ignore
    tableColumns = tableData.columns || [];
    // @ts-ignore
    tableRows = tableData.data || [];
  } else {
    // Old data structure format
    console.log('Processing legacy table format');
    // @ts-ignore - Safely access properties that might not be defined on the type
    tableTitle = tableData.title || '';
    // @ts-ignore
    tableColumns = tableData.headers?.map((header: string) => ({ name: header })) || [];
    // @ts-ignore
    tableRows = tableData.rows || [];
  }

  console.log('Table columns:', tableColumns);
  console.log('Table rows:', tableRows);

  return (
    <Paper elevation={2} sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h6" gutterBottom>
        {tableTitle}
      </Typography>
      <TableContainer>
        {tableColumns.length > 0 && tableRows.length > 0 ? (
          <Table size="small">
            <TableHead>
              <TableRow>
                {tableColumns.map((column: any, index: number) => (
                  <TableCell 
                    key={index} 
                    align={index === 0 ? 'left' : 'right'}
                    sx={{ fontWeight: 'bold' }}
                  >
                    {column.name}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {tableRows.map((row: any, rowIndex: number) => (
                <TableRow key={rowIndex}>
                  {Array.isArray(row) ? (
                    // Handle old format (array of values)
                    row.map((cell: any, cellIndex: number) => {
                      // Get column type if available
                      const columnType = tableColumns[cellIndex]?.type;
                      return (
                        <TableCell 
                          key={cellIndex} 
                          align={cellIndex === 0 ? 'left' : 'right'}
                          sx={cellIndex === 0 ? { fontWeight: 'medium' } : {}}
                        >
                          {formatValue(cell, columnType)}
                        </TableCell>
                      );
                    })
                  ) : (
                    // Handle new format (object with keys)
                    tableColumns.map((column: any, cellIndex: number) => {
                      const value = row[column.id];
                      return (
                        <TableCell 
                          key={cellIndex} 
                          align={cellIndex === 0 ? 'left' : 'right'}
                          sx={cellIndex === 0 ? { fontWeight: 'medium' } : {}}
                        >
                          {formatValue(value, column.type)}
                        </TableCell>
                      );
                    })
                  )}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Typography variant="body2" sx={{ py: 2, textAlign: 'center' }}>
            No data to display
          </Typography>
        )}
      </TableContainer>
    </Paper>
  );
};

export default TableRenderer; 