import React, { useState } from 'react';
import { ThemeProvider, createTheme, Container, CssBaseline, AppBar, Toolbar, Typography, Box } from '@mui/material';
import { FullTearsheet } from './types';
import FileUpload from './components/FileUpload';
import Dashboard from './components/Dashboard';

// Create a theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
});

function App() {
  const [tearsheetData, setTearsheetData] = useState<FullTearsheet | null>(null);

  const handleFileLoaded = (data: FullTearsheet) => {
    console.log('File loaded successfully:', data);
    setTearsheetData(data);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6">EpochFolio Dashboard</Typography>
        </Toolbar>
      </AppBar>
      
      <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
        {!tearsheetData ? (
          <Box sx={{ mt: 4, p: 2 }}>
            <Typography variant="h5" gutterBottom>
              Upload Tearsheet
            </Typography>
            <FileUpload onFileLoaded={handleFileLoaded} />
            <Box sx={{ mt: 2 }}>
              <Typography variant="body2" color="text.secondary">
                You can download a sample tearsheet from{' '}
                <a href="/sample_tearsheet.json" download>
                  here
                </a>
              </Typography>
            </Box>
          </Box>
        ) : (
          <Box sx={{ mt: 4 }}>
            <Dashboard data={tearsheetData} />
          </Box>
        )}
      </Container>
    </ThemeProvider>
  );
}

export default App;
