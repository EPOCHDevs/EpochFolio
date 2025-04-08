import React, { useState, useRef } from 'react';
import { Box, Button, Typography, Alert, CircularProgress } from '@mui/material';
import { FullTearsheet } from '../types';

interface FileUploadProps {
  onFileLoaded: (data: FullTearsheet) => void;
}

const FileUpload: React.FC<FileUploadProps> = ({ onFileLoaded }) => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const validateTearsheetData = (data: any): data is FullTearsheet => {
    // Clear previous errors
    setError(null);
    
    if (!data || typeof data !== 'object') {
      setError('Invalid JSON: Expected an object');
      return false;
    }

    // Check for required sections
    const requiredCategories = [
      'strategy_benchmark',
      'risk_analysis',
      'returns_distribution',
      'positions',
      'transactions',
      'round_trip'
    ];

    // Missing categories count (we'll allow some flexibility)
    let missingCategoriesCount = 0;
    
    for (const category of requiredCategories) {
      if (!data[category]) {
        console.warn(`Missing category: ${category}`);
        missingCategoriesCount++;
      } else {
        // Check if category has the required arrays
        if (!Array.isArray(data[category].cards)) {
          console.warn(`${category}.cards is not an array`);
        }
        
        if (!Array.isArray(data[category].charts)) {
          console.warn(`${category}.charts is not an array`);
        }
        
        if (!Array.isArray(data[category].tables)) {
          console.warn(`${category}.tables is not an array`);
        }
      }
    }
    
    // If more than half the categories are missing, consider it invalid
    if (missingCategoriesCount > requiredCategories.length / 2) {
      setError(`Invalid tearsheet format: Missing too many required categories. Expected: ${requiredCategories.join(', ')}`);
      return false;
    }

    return true;
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      const file = files[0];
      if (file.type === 'application/json' || file.name.endsWith('.json')) {
        setSelectedFile(file);
        setError(null);
      } else {
        setSelectedFile(null);
        setError('Please select a JSON file');
      }
    }
  };

  const handleFileUpload = () => {
    if (selectedFile) {
      setLoading(true);
      const reader = new FileReader();
      
      reader.onload = (e) => {
        const content = e.target?.result as string;
        try {
          const json = JSON.parse(content);
          if (validateTearsheetData(json)) {
            onFileLoaded(json);
          }
        } catch (err) {
          setError('Error parsing JSON file: ' + (err instanceof Error ? err.message : String(err)));
        } finally {
          setLoading(false);
        }
      };
      
      reader.onerror = () => {
        setError('Error reading file');
        setLoading(false);
      };
      
      reader.readAsText(selectedFile);
    }
  };

  const handleSelectClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  return (
    <Box>
      <input
        type="file"
        accept=".json,application/json"
        ref={fileInputRef}
        onChange={handleFileChange}
        style={{ display: 'none' }}
      />
      
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
        <Button 
          variant="contained" 
          onClick={handleSelectClick}
          disabled={loading}
          sx={{ mr: 2 }}
        >
          Select File
        </Button>
        {selectedFile && (
          <Typography variant="body1">
            {selectedFile.name}
          </Typography>
        )}
      </Box>
      
      {selectedFile && (
        <Button
          variant="contained"
          color="primary"
          onClick={handleFileUpload}
          disabled={loading}
          sx={{ mr: 2 }}
        >
          {loading ? <CircularProgress size={24} color="inherit" /> : "Upload"}
        </Button>
      )}
      
      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}
      
      <Box sx={{ mt: 4 }}>
        <Typography variant="subtitle1" gutterBottom>
          Expected JSON Format:
        </Typography>
        <Box 
          component="pre" 
          sx={{ 
            p: 2, 
            bgcolor: 'grey.100', 
            borderRadius: 1, 
            overflowX: 'auto',
            fontSize: '0.75rem'
          }}
        >
{`{
  "strategy_benchmark": {
    "cards": [
      {
        "title": "Performance Metrics",
        "items": [
          { "name": "Return", "value": 0.1234 },
          { "name": "Volatility", "value": 0.0567 }
        ]
      }
    ],
    "charts": [
      {
        "title": "Cumulative Returns",
        "type": "LINE",
        "data": [...],
        "options": { ... }
      }
    ],
    "tables": [
      {
        "title": "Monthly Returns",
        "headers": ["Month", "Return"],
        "rows": [
          ["Jan 2023", 0.034],
          ["Feb 2023", -0.012]
        ]
      }
    ]
  },
  "risk_analysis": { ... },
  "returns_distribution": { ... },
  "positions": { ... },
  "transactions": { ... },
  "round_trip": { ... }
}`}
        </Box>
      </Box>
    </Box>
  );
};

export default FileUpload; 