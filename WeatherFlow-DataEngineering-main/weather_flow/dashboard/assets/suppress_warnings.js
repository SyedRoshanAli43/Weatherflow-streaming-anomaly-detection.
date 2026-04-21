// Suppress React console warnings
(function() {
  // Store original console.error
  const originalConsoleError = console.error;
  
  // Override console.error
  console.error = function(message) {
    // Filter out React defaultProps warnings
    if (typeof message === 'string' && 
        (message.includes('defaultProps') || 
         message.includes('Warning:') ||
         message.includes('React.createClass'))) {
      return;
    }
    
    // Pass through other errors to original console.error
    return originalConsoleError.apply(console, arguments);
  };
  
  // Also suppress some warnings
  const originalConsoleWarn = console.warn;
  console.warn = function(message) {
    // Filter out React warnings
    if (typeof message === 'string' && 
        (message.includes('Warning:') || 
         message.includes('React'))) {
      return;
    }
    
    // Pass through other warnings
    return originalConsoleWarn.apply(console, arguments);
  };
})();
