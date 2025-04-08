import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';

console.log('Starting application...');

const rootElement = document.getElementById('root');
console.log('Root element found:', rootElement !== null);

if (rootElement) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
  console.log('React render called');
} else {
  console.error('Root element not found. Check if there is a div with id="root" in your HTML');
}
