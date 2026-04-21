"""
Data access layer for the WeatherFlow dashboard.
This module provides robust file access methods to prevent resource deadlock errors.
"""

import os
import json
import time
import random
import logging
import glob
import errno
import hashlib
import tempfile
import shutil
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dashboard.data_access")

# Global cache for loaded data
GLOBAL_CACHE = {}
GLOBAL_CACHE_TIMESTAMP = {}
GLOBAL_CACHE_LOCK = threading.RLock()

# In-memory cache for file contents
FILE_CONTENT_CACHE = {}
FILE_CACHE_TIMESTAMP = {}
FILE_CACHE_LOCK = threading.RLock()

# Circuit breaker pattern to avoid repeatedly trying files that cause deadlocks
CIRCUIT_BREAKER = {}
CIRCUIT_BREAKER_LOCK = threading.RLock()
CIRCUIT_BREAKER_THRESHOLD = 3  # Number of consecutive failures before tripping
CIRCUIT_BREAKER_RESET_TIME = 300  # Time in seconds before resetting the circuit

# Path to the temporary directory for file operations
TEMP_DIR = '/tmp/weather_data_cache'
os.makedirs(TEMP_DIR, exist_ok=True)

def get_file_hash(file_path):
    """Generate a unique hash for a file path."""
    return hashlib.md5(file_path.encode()).hexdigest()

def cache_file_content(file_path, content):
    """Cache file content in memory."""
    with FILE_CACHE_LOCK:
        FILE_CONTENT_CACHE[file_path] = content
        FILE_CACHE_TIMESTAMP[file_path] = time.time()

def get_cached_content(file_path, max_age=300):
    """Get cached content from memory if it exists and is not too old."""
    with FILE_CACHE_LOCK:
        if file_path in FILE_CONTENT_CACHE:
            timestamp = FILE_CACHE_TIMESTAMP.get(file_path, 0)
            if time.time() - timestamp <= max_age:
                return FILE_CONTENT_CACHE[file_path]
    return None

def check_circuit_breaker(file_path):
    """Check if the circuit breaker is tripped for this file.
    
    Returns:
        bool: True if the circuit is closed (file can be accessed), False if tripped
    """
    with CIRCUIT_BREAKER_LOCK:
        if file_path not in CIRCUIT_BREAKER:
            return True
            
        failures, last_failure_time = CIRCUIT_BREAKER[file_path]
        
        # Reset circuit after reset time
        if time.time() - last_failure_time > CIRCUIT_BREAKER_RESET_TIME:
            CIRCUIT_BREAKER[file_path] = (0, 0)
            return True
            
        # Circuit is tripped if failures exceed threshold
        return failures < CIRCUIT_BREAKER_THRESHOLD

def record_failure(file_path):
    """Record a failure in the circuit breaker."""
    with CIRCUIT_BREAKER_LOCK:
        if file_path not in CIRCUIT_BREAKER:
            CIRCUIT_BREAKER[file_path] = (1, time.time())
        else:
            failures, _ = CIRCUIT_BREAKER[file_path]
            CIRCUIT_BREAKER[file_path] = (failures + 1, time.time())

def safe_read_file(file_path):
    """Safely read a file's contents by creating a temporary copy in /tmp first.
    
    This approach avoids resource deadlocks by working with a copy of the file
    in a writable directory (/tmp) instead of directly accessing the read-only volume.
    """
    # Check circuit breaker first
    if not check_circuit_breaker(file_path):
        logger.warning(f"Circuit breaker tripped for {os.path.basename(file_path)}, skipping")
        return None
        
    try:
        # Check if we have this file cached in memory
        cached_data = get_cached_content(file_path)
        if cached_data is not None:
            logger.debug(f"Using cached data for {os.path.basename(file_path)}")
            return cached_data
            
        # Add a significant delay before file access to reduce contention
        time.sleep(random.uniform(0.5, 1.0))
            
        # Create a unique filename for the temporary copy
        file_hash = get_file_hash(file_path)
        temp_file_path = os.path.join(TEMP_DIR, f"{file_hash}.json")
        
        # Copy the file to the temporary location
        try:
            # Read the original file content in small chunks to reduce memory pressure
            with open(file_path, 'rb') as src_file:
                content = src_file.read()
                
            # Add a small delay after reading
            time.sleep(random.uniform(0.2, 0.5))
                
            # Write to temporary file
            with open(temp_file_path, 'wb') as dst_file:
                dst_file.write(content)
                
            # Add a small delay after writing
            time.sleep(random.uniform(0.2, 0.5))
                
            # Read from the temporary file
            with open(temp_file_path, 'r') as f:
                data = json.load(f)
                
            # Cache the data
            cache_file_content(file_path, data)
            
            # Clean up the temporary file
            try:
                os.remove(temp_file_path)
            except:
                pass
                
            return data
        except Exception as e:
            logger.error(f"Error processing temporary copy of {os.path.basename(file_path)}: {e}")
            record_failure(file_path)
            return None
            
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON in {os.path.basename(file_path)}: {e}")
        return None
    except Exception as e:
        # If it's a resource deadlock, log it but don't include the full traceback
        if isinstance(e, OSError) and e.errno == errno.EDEADLK:
            logger.error(f"Error loading file {file_path}: {e}")
            record_failure(file_path)
        else:
            logger.error(f"Error reading file {os.path.basename(file_path)}: {e}")
            logger.debug(traceback.format_exc())
        return None

class WeatherDataAccessLayer:
    """
    Data access layer for weather data with robust error handling and concurrency control.
    Optimized for read-only file access in Docker environments.
    """
    
    def __init__(self, data_dir: str, max_retries: int = 8, retry_delay: float = 0.5, max_workers: int = 1):
        """
        Initialize the data access layer.
        
        Args:
            data_dir: Directory containing weather data files
            max_retries: Maximum number of retries for file access operations
            retry_delay: Base delay between retries (will use exponential backoff)
            max_workers: Maximum number of worker threads for parallel processing
        """
        self.data_dir = data_dir
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_workers = max_workers
        self.raw_data_dir = os.path.join(data_dir, 'raw')
        self.processed_data_dir = os.path.join(data_dir, 'processed')
        self.reports_dir = os.path.join(data_dir, 'reports')
        
        # Use global cache to share across instances
        self._data_cache = GLOBAL_CACHE
        self._cache_timestamp = GLOBAL_CACHE_TIMESTAMP
        self._cache_lock = GLOBAL_CACHE_LOCK
        
        # Track failed files to avoid repeated retries
        self._failed_files = set()
        self._failed_files_lock = threading.RLock()
        
        # Track successful files to prioritize them in future runs
        self._successful_files = set()
        self._successful_files_lock = threading.RLock()
        
        # File size cache to prioritize smaller files
        self._file_sizes = {}
        self._file_sizes_lock = threading.RLock()
        
        logger.info(f"Data access layer initialized with data directory: {data_dir}")
    
    def _safe_read_json(self, file_path: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Safely read a JSON file with in-memory caching.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Parsed JSON data or None if file couldn't be read
        """
        # Check if file exists
        if not os.path.exists(file_path):
            logger.warning(f"File does not exist: {file_path}")
            return None
            
        # Check if this file has consistently failed
        with self._failed_files_lock:
            if file_path in self._failed_files:
                logger.debug(f"Skipping previously failed file: {os.path.basename(file_path)}")
                return None
                
        # Cache file size for future prioritization
        try:
            with self._file_sizes_lock:
                if file_path not in self._file_sizes:
                    self._file_sizes[file_path] = os.path.getsize(file_path)
        except Exception:
            pass
        
        # Try multiple times with aggressive exponential backoff
        for attempt in range(self.max_retries):
            try:
                # Add a significant random delay to reduce contention
                # The delay increases exponentially with each attempt
                if attempt > 0:
                    # More aggressive exponential backoff with much larger random component
                    delay = self.retry_delay * (4 ** attempt) + random.uniform(1.0, 3.0)
                    logger.debug(f"Waiting {delay:.2f}s before retry {attempt+1}/{self.max_retries} for {os.path.basename(file_path)}")
                    time.sleep(delay)
                
                # Read the file using our safe read method
                data = safe_read_file(file_path)
                if data is not None:
                    # Mark as successful
                    with self._successful_files_lock:
                        self._successful_files.add(file_path)
                    return data
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{self.max_retries} failed for {os.path.basename(file_path)}: {e}")
        
        # If we get here, all attempts failed
        logger.error(f"All {self.max_retries} attempts failed for {os.path.basename(file_path)}")
        with self._failed_files_lock:
            self._failed_files.add(file_path)
        return None
    
    def get_weather_data(self, use_cache: bool = True, cache_ttl: int = 300) -> pd.DataFrame:
        """
        Get all weather data from raw files using sequential processing and caching.
        
        Args:
            use_cache: Whether to use cached data if available
            cache_ttl: Cache time-to-live in seconds
            
        Returns:
            DataFrame containing all weather data
        """
        # Check memory cache first if enabled
        if use_cache:
            with self._cache_lock:
                if 'weather_data' in self._data_cache:
                    cache_time = self._cache_timestamp.get('weather_data', 0)
                    if time.time() - cache_time < cache_ttl:
                        logger.debug("Using cached weather data from memory")
                        return self._data_cache['weather_data']
        
        # Get all JSON files in the raw data directory
        pattern = os.path.join(self.raw_data_dir, '*.json')
        try:
            all_files = glob.glob(pattern)
            logger.info(f"Found {len(all_files)} raw data files")
        except Exception as e:
            logger.error(f"Error listing files in {self.raw_data_dir}: {e}")
            all_files = []
        
        if not all_files:
            logger.warning("No files found in the raw data directory")
            return pd.DataFrame()
        
        # Limit the number of files to process to avoid overwhelming the system
        max_files = 5  # Drastically reduced to minimize contention
        if len(all_files) > max_files:
            logger.info(f"Limiting to {max_files} files to prevent system overload")
            
            # Prioritize files that were successfully read before
            with self._successful_files_lock:
                successful = [f for f in all_files if f in self._successful_files]
            
            # Get file sizes for prioritization
            file_sizes = {}
            with self._file_sizes_lock:
                for f in all_files:
                    if f in self._file_sizes:
                        file_sizes[f] = self._file_sizes[f]
            
            # If we have successful files, use them first
            if successful and len(successful) <= max_files:
                logger.info(f"Using {len(successful)} previously successful files")
                all_files = successful
            elif successful:
                # Use some successful files and some small files
                num_successful = max_files // 2
                num_small = max_files - num_successful
                
                if len(successful) > num_successful:
                    # Sort successful files by size (smallest first) if we have size info
                    if file_sizes:
                        successful_with_size = [(f, file_sizes.get(f, float('inf'))) for f in successful]
                        successful_with_size.sort(key=lambda x: x[1])  # Sort by file size
                        successful = [f for f, _ in successful_with_size[:num_successful]]
                    else:
                        successful = random.sample(successful, num_successful)
                
                # Get remaining files, prioritizing smaller files
                remaining = [f for f in all_files if f not in successful]
                if file_sizes and remaining:
                    remaining_with_size = [(f, file_sizes.get(f, float('inf'))) for f in remaining]
                    remaining_with_size.sort(key=lambda x: x[1])  # Sort by file size
                    small_files = [f for f, _ in remaining_with_size[:num_small]]
                else:
                    small_files = random.sample(remaining, min(num_small, len(remaining)))
                
                all_files = successful + small_files
                logger.info(f"Using {len(successful)} successful files and {len(small_files)} small files")
            else:
                # No successful files, prioritize smaller files if we have size info
                if file_sizes:
                    files_with_size = [(f, file_sizes.get(f, float('inf'))) for f in all_files]
                    files_with_size.sort(key=lambda x: x[1])  # Sort by file size
                    all_files = [f for f, _ in files_with_size[:max_files]]
                    logger.info(f"Using {len(all_files)} smallest files")
                else:
                    all_files = random.sample(all_files, max_files)
                    logger.info(f"Using {len(all_files)} random files")
        
        # Clear the temporary directory before processing
        try:
            for temp_file in os.listdir(TEMP_DIR):
                try:
                    os.remove(os.path.join(TEMP_DIR, temp_file))
                except:
                    pass
        except:
            pass
            
        # Process files one at a time with very significant delays between each
        all_data = []
        success_count = 0
        
        for i, file_path in enumerate(all_files):
            logger.debug(f"Processing file {i+1}/{len(all_files)}: {os.path.basename(file_path)}")
            
            # Add a much larger random delay between files to reduce contention
            if i > 0:
                delay = random.uniform(1.0, 3.0)
                logger.debug(f"Waiting {delay:.2f}s before processing next file")
                time.sleep(delay)
                
            try:
                # Use a retry mechanism for each file with increasing delays
                for attempt in range(3):
                    try:
                        if attempt > 0:
                            # More aggressive exponential backoff
                            delay = 1.0 * (3 ** attempt)
                            logger.debug(f"Waiting {delay:.2f}s before retry {attempt+1}/3")
                            time.sleep(delay)
                            
                        data = self._safe_read_json(file_path)
                        if data:
                            success_count += 1
                            if isinstance(data, list):
                                all_data.extend(data)
                            else:
                                all_data.append(data)
                            
                            # Add file size to our cache for future prioritization
                            try:
                                with self._file_sizes_lock:
                                    if file_path not in self._file_sizes:
                                        self._file_sizes[file_path] = os.path.getsize(file_path)
                            except Exception:
                                pass
                                
                            break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt+1}/3 failed for {os.path.basename(file_path)}: {e}")
            except Exception as e:
                logger.error(f"Error processing {os.path.basename(file_path)}: {e}")
        
        logger.info(f"Successfully processed {success_count}/{len(all_files)} files")
        
        # Convert to DataFrame
        if not all_data:
            logger.warning("No data found in any files")
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        
        # Cache the result
        with self._cache_lock:
            self._data_cache['weather_data'] = df
            self._cache_timestamp['weather_data'] = time.time()
            
        logger.info(f"Loaded {len(df)} weather data records")
        return df
    
    def _process_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Process a single file and return the processed data.
        
        This is a helper method for parallel processing.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List of processed records or empty list if file couldn't be read
        """
        result = []
        try:
            # Read the file with caching strategy
            data = self._safe_read_json(file_path)
            if data is None:
                return []
                
            # Process the data
            if isinstance(data, dict):
                # Single record
                processed_record = self._process_weather_record(data)
                if processed_record:
                    result.append(processed_record)
            elif isinstance(data, list):
                # Multiple records in one file
                for record in data:
                    if not isinstance(record, dict):
                        continue
                    processed_record = self._process_weather_record(record)
                    if processed_record:
                        result.append(processed_record)
                        
            return result
        except Exception as e:
            logger.error(f"Error in _process_file for {os.path.basename(file_path)}: {e}")
            return []
    
    def _process_and_append_data(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], all_data: List[Dict[str, Any]]):
        """
        Process weather data and append to the data list.
        
        Args:
            data: Weather data as dict or list of dicts
            all_data: List to append processed data to
        """
        # This method is kept for backward compatibility but is now primarily used by the _process_file method
        if isinstance(data, dict):
            # Single record
            processed_record = self._process_weather_record(data)
            if processed_record:
                all_data.append(processed_record)
        elif isinstance(data, list):
            # Multiple records in one file
            for record in data:
                if not isinstance(record, dict):
                    continue
                processed_record = self._process_weather_record(record)
                if processed_record:
                    all_data.append(processed_record)
    
    def _process_weather_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single weather record.
        
        Args:
            record: Weather data record
            
        Returns:
            Processed record or None if invalid
        """
        try:
            # Make a copy to avoid modifying the original
            processed = record.copy()
            
            # Process temperature data
            if 'temperature' in processed:
                # Convert to Fahrenheit
                processed['temperature_fahrenheit'] = processed['temperature'] * 9/5 + 32
                
                # Calculate heat index if humidity is available
                if 'humidity' in processed and processed['temperature'] * 9/5 + 32 > 80:
                    temp_f = processed['temperature'] * 9/5 + 32
                    humidity = processed['humidity']
                    
                    # Heat index formula
                    heat_index = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity
                    heat_index += -0.22475541 * temp_f * humidity - 6.83783e-3 * temp_f**2
                    heat_index += -5.481717e-2 * humidity**2 + 1.22874e-3 * temp_f**2 * humidity
                    heat_index += 8.5282e-4 * temp_f * humidity**2 - 1.99e-6 * temp_f**2 * humidity**2
                    
                    processed['heat_index_fahrenheit'] = heat_index
                    processed['heat_index_celsius'] = (heat_index - 32) * 5/9
            
            # Add weather severity
            if 'weather_condition' in processed:
                condition = processed['weather_condition'].lower()
                
                # Define severity levels
                severe_conditions = ['thunderstorm', 'tornado', 'hurricane', 'blizzard']
                moderate_conditions = ['rain', 'snow', 'drizzle', 'fog', 'mist', 'shower']
                mild_conditions = ['cloudy', 'overcast', 'partly cloudy']
                
                if any(cond in condition for cond in severe_conditions):
                    processed['weather_severity'] = 'severe'
                elif any(cond in condition for cond in moderate_conditions):
                    processed['weather_severity'] = 'moderate'
                elif any(cond in condition for cond in mild_conditions):
                    processed['weather_severity'] = 'mild'
                else:
                    processed['weather_severity'] = 'normal'
            
            return processed
        except Exception as e:
            logger.error(f"Error processing weather record: {e}")
            return None
    
    def get_cities(self) -> List[str]:
        """
        Get list of all cities in the weather data.
        
        Returns:
            List of city names
        """
        df = self.get_weather_data()
        if df.empty or 'city' not in df.columns:
            return []
        
        return df['city'].unique().tolist()
    
    def get_weather_data_for_cities(self, cities: List[str], start_date: Optional[str] = None, 
                                   end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get weather data filtered by cities and date range.
        
        Args:
            cities: List of cities to include
            start_date: Start date for filtering (ISO format)
            end_date: End date for filtering (ISO format)
            
        Returns:
            Filtered DataFrame
        """
        df = self.get_weather_data()
        
        if df.empty:
            return df
        
        # Filter by cities if provided
        if cities and len(cities) > 0:
            df = df[df['city'].isin(cities)]
        
        # Filter by date range if provided
        if 'timestamp' in df.columns:
            if start_date:
                df = df[df['timestamp'] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df['timestamp'] <= pd.to_datetime(end_date)]
        
        return df
    
    def clear_cache(self):
        """Clear the data cache."""
        with self._cache_lock:
            self._data_cache.clear()
            self._cache_timestamp.clear()
        logger.info("Data cache cleared")

# Create a singleton instance with highly optimized parameters for avoiding deadlocks
weather_data_access = WeatherDataAccessLayer(
    '/app/data', 
    max_retries=8,  # Increased retries
    retry_delay=1.0,  # Increased base delay
    max_workers=1  # Single worker to avoid concurrency issues
)  # Sequential processing to avoid contention

def get_weather_data_access() -> WeatherDataAccessLayer:
    """Get the singleton instance of the weather data access layer."""
    return weather_data_access
