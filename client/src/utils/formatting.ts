/**
 * Utility functions for formatting scalar values in consistent ways
 * across different components (cards, tables, charts)
 */

import { EpochFolioType } from '../types';

type Scalar = string | number | boolean;

/**
 * Format a scalar value based on its type
 * @param value The value to format
 * @param type The type of the value (from EpochFolioType enum)
 * @returns Formatted string representation of the value
 */
export const formatValue = (value: Scalar, type?: string): string => {
  if (value === null || value === undefined) {
    return '';
  }

  // Handle special case for boolean values
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }

  // Convert numbers and strings to appropriate format based on type
  switch (type) {
    case EpochFolioType.PERCENT:
      // Format as percentage
      const numValue = typeof value === 'string' ? parseFloat(value) : value;
      if (typeof numValue !== 'number' || isNaN(numValue)) return String(value);

      // If already in percentage form (e.g., 25 for 25%)
      return `${numValue.toFixed(2)}%`;

    case EpochFolioType.DECIMAL:
      // Format with 2 decimal places
      const decValue = typeof value === 'string' ? parseFloat(value) : value;
      if (typeof decValue !== 'number' || isNaN(decValue)) return String(value);
      return decValue.toFixed(2);

    case EpochFolioType.INTEGER:
      // Format as integer
      const intValue = typeof value === 'string' ? parseInt(value, 10) : value;
      if (typeof intValue !== 'number' || isNaN(intValue)) return String(value);
      return intValue.toLocaleString();

    case EpochFolioType.DATE:
      // Format date
      try {
        if (typeof value === 'number') {
          // Assume timestamp in milliseconds
          return new Date(value).toLocaleDateString();
        }
        return new Date(String(value)).toLocaleDateString();
      } catch (e) {
        return String(value);
      }

    case EpochFolioType.DATETIME:
      // Format datetime
      try {
        if (typeof value === 'number') {
          // Assume timestamp in milliseconds
          return new Date(value).toLocaleString();
        }
        return new Date(String(value)).toLocaleString();
      } catch (e) {
        return String(value);
      }

    case EpochFolioType.MONETARY:
      // Format as currency
      const moneyValue = typeof value === 'string' ? parseFloat(value) : value;
      if (typeof moneyValue !== 'number' || isNaN(moneyValue)) return String(value);
      return moneyValue.toLocaleString('en-US', {
        style: 'currency',
        currency: 'USD', // Default to USD, can be made configurable
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      });

    case EpochFolioType.STRING:
      // Just return the string
      return String(value);

    case EpochFolioType.DAY_DURATION:
      // Format as days
      const days = typeof value === 'string' ? parseFloat(value) : value;
      if (typeof days !== 'number' || isNaN(days)) return String(value);
      return days === 1 ? '1 day' : `${days} days`;

    case EpochFolioType.DURATION:
      // Format duration (assuming nanoseconds input)
      const nanoseconds = typeof value === 'string' ? parseFloat(value) : value;
      if (typeof nanoseconds !== 'number' || isNaN(nanoseconds)) return String(value);
      return formatDuration(nanoseconds / 1e9); // Convert nanoseconds to seconds for formatDuration

    default:
      // Default formatting based on value type
      if (typeof value === 'number') {
        // Format with 2 decimal places
        return value.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        });
      }
      return String(value);
  }
};

/**
 * Format a duration in seconds to a human-readable string
 * @param seconds Duration in seconds
 * @returns Formatted duration string
 */
export const formatDuration = (seconds: number): string => {
  if (seconds === 0) return '0 seconds';

  // Handle negative durations
  const negative = seconds < 0;
  seconds = Math.abs(seconds);

  // For very small durations (less than a millisecond)
  if (seconds < 0.001) {
    const microseconds = Math.round(seconds * 1000000);
    return `${negative ? '-' : ''}${microseconds} µs`;
  }

  // For millisecond durations
  if (seconds < 1) {
    const milliseconds = Math.round(seconds * 1000);
    return `${negative ? '-' : ''}${milliseconds} ms`;
  }

  // For day durations
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;
  return `${negative ? '-' : ''}${days}d ${hours}:${minutes}:${remainingSeconds.toFixed(0)}`;
}; 