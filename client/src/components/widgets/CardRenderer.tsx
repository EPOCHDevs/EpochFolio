import React, { useEffect } from 'react';
import { Paper, Typography, Grid, Box } from '@mui/material';
import { Card, CardData } from '../../types';
import { formatValue } from '../../utils/formatting';

interface CardRendererProps {
  cardData: Card;
}

const CardRenderer: React.FC<CardRendererProps> = ({ cardData }) => {
  useEffect(() => {
    // Debug log to check data structure
    console.log('Card data received:', cardData);
  }, [cardData]);

  if (!cardData) {
    return (
      <Paper elevation={0} sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
        <Typography variant="body1">No card data available</Typography>
      </Paper>
    );
  }

  // Check for new card structure with data array
  let cardItems: CardData[] = [];
  let groupSize = 2; // Default: 2 columns

  if ((cardData as any).data && Array.isArray((cardData as any).data)) {
    // New structure
    console.log('Processing new card format');
    // @ts-ignore
    cardItems = cardData.data || [];
    // @ts-ignore
    groupSize = cardData.group_size || 2;
  } else {
    // Old structure
    console.log('Processing legacy card format');
    // @ts-ignore
    cardItems = cardData.items || [];
  }

  // Render card items in a 2-column layout
  const renderCardItems = () => {
    // If no items, show empty message
    if (!cardItems.length) {
      return (
        <Grid sx={{ width: '100%', p: 1, boxSizing: 'border-box' }}>
          <Typography variant="body2" align="center">
            No items to display
          </Typography>
        </Grid>
      );
    }
    
    // Keep track of processed items
    const processedItems: boolean[] = Array(cardItems.length).fill(false);
    
    // Create a flex container for all items
    return (
      <Box sx={{ 
        display: 'flex', 
        flexWrap: 'wrap', 
        width: '100%'
      }}>
        {/* First process grouped items */}
        {cardItems.reduce((result: React.ReactNode[], item, index) => {
          // Skip if already processed
          if (processedItems[index]) return result;
          
          const group = item.group !== undefined ? item.group : null;
          
          // If this is a grouped item
          if (group !== null) {
            // Find all items in the same group
            const groupItems = cardItems
              .map((item, idx) => ({ item, idx }))
              .filter(({ item }) => item.group === group);
            
            // Mark all as processed
            groupItems.forEach(({ idx }) => {
              processedItems[idx] = true;
            });
            
            // Add the group container
            result.push(
              <Box
                key={`group-${group}-${index}`}
                sx={{
                  width: '100%',
                  p: 1,
                  boxSizing: 'border-box'
                }}
              >
                <Box sx={{ 
                  p: 2,
                  border: '1px solid rgba(0, 0, 0, 0.12)',
                  borderRadius: '4px',
                  backgroundColor: 'rgba(0, 0, 0, 0.01)'
                }}>
                  <Box sx={{ 
                    display: 'flex', 
                    flexWrap: 'wrap', 
                    width: '100%'
                  }}>
                    {groupItems.map(({ item, idx }) => (
                      <Box
                        key={`group-item-${idx}`}
                        sx={{
                          width: '50%', // 2 columns in the group too
                          p: 1,
                          boxSizing: 'border-box'
                        }}
                      >
                        <Box sx={{ 
                          p: 1.5,
                          width: '100%',
                          borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
                          backgroundColor: 'rgba(0, 0, 0, 0.02)',
                          borderRadius: '4px'
                        }}>
                          <Typography 
                            variant="body2" 
                            color="text.secondary" 
                            gutterBottom 
                            sx={{ 
                              textAlign: 'left',
                              display: 'block',
                              width: '100%',
                              mb: 0.5
                            }}
                          >
                            {item.title}
                          </Typography>
                          <Typography 
                            variant="h6" 
                            sx={{ 
                              fontWeight: 'medium', 
                              textAlign: 'left',
                              display: 'block',
                              width: '100%'
                            }}
                          >
                            {formatValue(item.value, item.type)}
                          </Typography>
                        </Box>
                      </Box>
                    ))}
                  </Box>
                </Box>
              </Box>
            );
          }
          
          return result;
        }, [])}
        
        {/* Then process individual items */}
        {cardItems.map((item, index) => {
          // Skip if already processed (part of a group)
          if (processedItems[index]) return null;
          
          return (
            <Box
              key={`item-${index}`}
              sx={{
                width: '50%', // 2 columns
                p: 1,
                boxSizing: 'border-box'
              }}
            >
              <Box sx={{ 
                p: 1.5,
                width: '100%',
                borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
                backgroundColor: 'rgba(0, 0, 0, 0.02)',
                borderRadius: '4px'
              }}>
                <Typography 
                  variant="body2" 
                  color="text.secondary" 
                  gutterBottom 
                  sx={{ 
                    textAlign: 'left',
                    display: 'block',
                    width: '100%',
                    mb: 0.5
                  }}
                >
                  {item.title}
                </Typography>
                <Typography 
                  variant="h6" 
                  sx={{ 
                    fontWeight: 'medium', 
                    textAlign: 'left',
                    display: 'block',
                    width: '100%'
                  }}
                >
                  {formatValue(item.value, item.type)}
                </Typography>
              </Box>
            </Box>
          );
        })}
      </Box>
    );
  };

  return (
    <Paper elevation={0} sx={{ 
      height: '100%', 
      display: 'flex', 
      flexDirection: 'column',
      border: '1px solid rgba(0, 0, 0, 0.12)',
      borderRadius: 1,
      p: 1
    }}>
      <Box sx={{ flexGrow: 1, width: '100%' }}>
        {renderCardItems()}
      </Box>
    </Paper>
  );
};

export default CardRenderer; 