import React, { useState } from 'react';
import { Box, Tabs, Tab, Typography, Paper, Divider, Container } from '@mui/material';
import { FullTearsheet } from '../types';
import CardRenderer from './widgets/CardRenderer';
import ChartRenderer from './widgets/ChartRenderer';
import TableRenderer from './widgets/TableRenderer';

interface DashboardProps {
  data: FullTearsheet;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

const TabPanel = (props: TabPanelProps) => {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      {...other}
      style={{ width: '100%' }}
    >
      {value === index && <Box sx={{ width: '100%' }}>{children}</Box>}
    </div>
  );
};

// Maps category keys to display names
const categoryDisplayNames: Record<string, string> = {
  'strategy_benchmark': 'STRATEGY & BENCHMARK',
  'risk_analysis': 'RISK ANALYSIS',
  'returns_distribution': 'RETURNS DISTRIBUTION',
  'positions': 'POSITIONS',
  'transactions': 'TRANSACTIONS',
  'round_trip': 'ROUND TRIP'
};

// Section components
const ChartsSection = ({ charts }: { charts?: any[] }) => {
  if (!charts || charts.length === 0) return null;
  
  return (
    <Box sx={{ mb: 4, width: '100%' }}>
      <Typography variant="h6" gutterBottom sx={{ 
        borderBottom: '1px solid #e0e0e0',
        pb: 1, 
        mb: 3
      }}>
        Charts
      </Typography>
      <Box sx={{ width: '100%' }}>
        {charts.map((chart: any, chartIndex: number) => (
          <Box
            key={chartIndex}
            sx={{
              width: '100%',
              mb: 4,
              boxSizing: 'border-box',
              minHeight: '300px',
              border: '1px solid #e0e0e0',
              borderRadius: 1,
              p: 2
            }}
          >
            <ChartRenderer chartData={chart} />
          </Box>
        ))}
      </Box>
    </Box>
  );
};

const CardsSection = ({ cards }: { cards?: any[] }) => {
  if (!cards || cards.length === 0) return null;
  
  return (
    <Box sx={{ mb: 4, width: '100%' }}>
      <Box sx={{ 
        display: 'flex', 
        flexDirection: 'row',
        flexWrap: 'wrap', 
        width: '100%'
      }}>
        {cards.map((card: any, cardIndex: number) => (
          <Box
            key={cardIndex}
            sx={{
              width: '50%',
              boxSizing: 'border-box',
              p: 1
            }}
          >
            <Box sx={{ height: '100%' }}>
              <CardRenderer cardData={card} />
            </Box>
          </Box>
        ))}
      </Box>
    </Box>
  );
};

const TablesSection = ({ tables }: { tables?: any[] }) => {
  if (!tables || tables.length === 0) return null;
  
  return (
    <Box sx={{ mb: 4, width: '100%' }}>
      <Box sx={{ width: '100%' }}>
        {tables.map((table: any, tableIndex: number) => (
          <Box
            key={tableIndex}
            sx={{
              width: '100%',
              mb: 2,
              boxSizing: 'border-box',
              border: '1px solid #e0e0e0',
              borderRadius: 1,
              p: 2
            }}
          >
            <TableRenderer tableData={table} />
          </Box>
        ))}
      </Box>
    </Box>
  );
};

const Dashboard: React.FC<DashboardProps> = ({ data }) => {
  const [currentTab, setCurrentTab] = useState(0);
  
  console.log('Dashboard rendering with data:', data);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setCurrentTab(newValue);
  };

  // Get all category keys
  const categories = Object.keys(data) as Array<keyof FullTearsheet>;

  if (!categories || categories.length === 0) {
    return (
      <Paper sx={{ p: 2, textAlign: 'center' }}>
        <Typography variant="h5">No data available</Typography>
      </Paper>
    );
  }

  return (
    <Box sx={{ 
      width: '100vw',
      maxWidth: '100vw',
      overflowX: 'hidden',
      position: 'relative',
      left: '50%',
      right: '50%',
      marginLeft: '-50vw',
      marginRight: '-50vw',
      boxSizing: 'border-box'
    }}>
      <Box sx={{ 
        borderBottom: 1, 
        borderColor: 'divider',
        backgroundColor: '#fff',
        position: 'sticky',
        top: 0,
        zIndex: 10,
        width: '100%'
      }}>
        <Box sx={{ maxWidth: '100%', px: 3 }}>
          <Typography variant="h4" component="h1" gutterBottom sx={{ pt: 2, pb: 1 }}>
            Portfolio Tearsheet
          </Typography>
          <Tabs
            value={currentTab}
            onChange={handleTabChange}
            aria-label="dashboard tabs"
            variant="scrollable"
            scrollButtons="auto"
            sx={{
              '& .MuiTab-root': {
                textTransform: 'uppercase',
                fontSize: '0.875rem',
                fontWeight: 500,
                minWidth: 'auto',
                px: 2
              }
            }}
          >
            {categories.map((category, index) => (
              <Tab 
                key={index} 
                label={categoryDisplayNames[category as string] || String(category)} 
                id={`tab-${index}`} 
                aria-controls={`tabpanel-${index}`} 
              />
            ))}
          </Tabs>
        </Box>
      </Box>

      <Box sx={{ maxWidth: '100%', px: 3 }}>
        {categories.map((category, index) => {
          const categoryData = data[category];
          
          if (!categoryData) {
            return (
              <TabPanel key={index} value={currentTab} index={index}>
                <Typography>No data available for this category</Typography>
              </TabPanel>
            );
          }
          
          return (
            <TabPanel key={index} value={currentTab} index={index}>
              <Box sx={{ 
                display: 'flex', 
                flexDirection: { xs: 'column', lg: 'row' },
                width: '100%',
                mt: 3
              }}>
                {/* Charts section (70% width on large screens) */}
                <Box sx={{ 
                  width: { xs: '100%', lg: '70%' }, 
                  pr: { xs: 0, lg: 2 }
                }}>
                  <ChartsSection charts={categoryData.charts} />
                </Box>
                
                {/* Cards and Tables section (30% width on large screens) */}
                <Box sx={{ 
                  width: { xs: '100%', lg: '30%' }
                }}>
                  <CardsSection cards={categoryData.cards} />
                  <TablesSection tables={categoryData.tables} />
                </Box>
              </Box>
            </TabPanel>
          );
        })}
      </Box>
    </Box>
  );
};

export default Dashboard; 