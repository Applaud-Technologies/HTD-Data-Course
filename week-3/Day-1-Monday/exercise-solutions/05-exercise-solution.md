# Solutions for Pandas for Data Engineering Exercises

## Exercise 1: Chunking and Memory Management

```python
import pandas as pd
import numpy as np

def calculate_category_averages(filename, chunk_size=100_000):
    """
    Process a large CSV file in chunks to calculate average values per category.
    
    Parameters:
    - filename: Path to the CSV file
    - chunk_size: Number of rows to process at once
    
    Returns:
    - Dictionary with category averages
    """
    # Initialize variables to track totals and counts
    category_sums = {}
    category_counts = {}
    
    # Process the file in chunks
    for chunk in pd.read_csv(filename, chunksize=chunk_size):
        # Group by category and calculate sum and count
        for category, group in chunk.groupby('category'):
            if category not in category_sums:
                category_sums[category] = 0
                category_counts[category] = 0
            
            category_sums[category] += group['value'].sum()
            category_counts[category] += len(group)
        
        # Free memory
        del chunk
    
    # Calculate averages
    category_averages = {
        category: category_sums[category] / category_counts[category]
        for category in category_sums
    }
    
    return category_averages
```

### Explanation:
This solution efficiently processes a large CSV file by:
1. Reading the file in manageable chunks to avoid memory issues
2. Maintaining running sums and counts for each category
3. Explicitly freeing memory after processing each chunk
4. Calculating final averages only after all chunks are processed

The approach is memory-efficient because it only keeps track of aggregated statistics (sums and counts) rather than storing all the data. This allows processing of files much larger than available RAM.

## Exercise 2: Data Type Optimization

```python
import pandas as pd
import numpy as np

def optimize_dataframe_memory(df):
    """
    Optimize the memory usage of a DataFrame by converting columns to appropriate data types.
    
    Parameters:
    - df: Input DataFrame
    
    Returns:
    - Optimized DataFrame with reduced memory usage
    """
    # Create a copy to avoid modifying the original
    df_optimized = df.copy()
    
    # Optimize integer columns
    int_columns = df_optimized.select_dtypes(include=['int64']).columns
    for col in int_columns:
        col_min = df_optimized[col].min()
        col_max = df_optimized[col].max()
        
        # Choose appropriate integer type
        if col_min >= 0:
            if col_max < 255:
                df_optimized[col] = df_optimized[col].astype(np.uint8)
            elif col_max < 65535:
                df_optimized[col] = df_optimized[col].astype(np.uint16)
            elif col_max < 4294967295:
                df_optimized[col] = df_optimized[col].astype(np.uint32)
        else:
            if col_min > -128 and col_max < 127:
                df_optimized[col] = df_optimized[col].astype(np.int8)
            elif col_min > -32768 and col_max < 32767:
                df_optimized[col] = df_optimized[col].astype(np.int16)
            elif col_min > -2147483648 and col_max < 2147483647:
                df_optimized[col] = df_optimized[col].astype(np.int32)
    
    # Optimize float columns
    float_columns = df_optimized.select_dtypes(include=['float64']).columns
    for col in float_columns:
        df_optimized[col] = df_optimized[col].astype(np.float32)
    
    # Convert string columns with few unique values to categorical
    obj_columns = df_optimized.select_dtypes(include=['object']).columns
    for col in obj_columns:
        num_unique = df_optimized[col].nunique()
        num_total = len(df_optimized)
        if num_unique / num_total < 0.5:  # If less than 50% unique values
            df_optimized[col] = df_optimized[col].astype('category')
    
    # Print memory usage comparison
    original_memory = df.memory_usage().sum() / 1024**2
    optimized_memory = df_optimized.memory_usage().sum() / 1024**2
    print(f"Original memory usage: {original_memory:.2f} MB")
    print(f"Optimized memory usage: {optimized_memory:.2f} MB")
    print(f"Memory reduced by: {(1 - optimized_memory/original_memory) * 100:.2f}%")
    
    return df_optimized
```

### Explanation:
This solution optimizes DataFrame memory usage by:
1. Analyzing each column's data range to determine the smallest possible data type
2. Converting integer columns to the appropriate sized int/uint types based on min/max values
3. Downgrading float64 columns to float32 to save memory
4. Converting string columns with low cardinality to categorical type
5. Providing a detailed report of memory savings

The optimization can significantly reduce memory usage, often by 50% or more, which is crucial when working with large datasets.

## Exercise 3: Vectorized Operations

```python
import pandas as pd
import numpy as np
import time

def compare_vectorization_performance(n=1_000_000):
    """
    Compare the performance of vectorized operations versus loops for data transformation.
    
    Parameters:
    - n: Number of rows in the test DataFrame
    
    Returns:
    - Dictionary with timing results
    """
    # Create test data
    df = pd.DataFrame({
        'value1': np.random.randn(n),
        'value2': np.random.randn(n),
        'value3': np.random.randn(n)
    })
    
    # Method 1: Using loops (slow)
    start_time = time.time()
    
    df_loop = df.copy()
    df_loop['sum'] = 0
    df_loop['product'] = 0
    df_loop['max_value'] = 0
    
    for i in range(len(df_loop)):
        df_loop.loc[i, 'sum'] = df_loop.loc[i, 'value1'] + df_loop.loc[i, 'value2'] + df_loop.loc[i, 'value3']
        df_loop.loc[i, 'product'] = df_loop.loc[i, 'value1'] * df_loop.loc[i, 'value2'] * df_loop.loc[i, 'value3']
        df_loop.loc[i, 'max_value'] = max(df_loop.loc[i, 'value1'], df_loop.loc[i, 'value2'], df_loop.loc[i, 'value3'])
    
    loop_time = time.time() - start_time
    
    # Method 2: Using apply (better but still not optimal)
    start_time = time.time()
    
    df_apply = df.copy()
    df_apply['sum'] = df_apply.apply(lambda row: row['value1'] + row['value2'] + row['value3'], axis=1)
    df_apply['product'] = df_apply.apply(lambda row: row['value1'] * row['value2'] * row['value3'], axis=1)
    df_apply['max_value'] = df_apply.apply(lambda row: max(row['value1'], row['value2'], row['value3']), axis=1)
    
    apply_time = time.time() - start_time
    
    # Method 3: Using vectorized operations (fastest)
    start_time = time.time()
    
    df_vectorized = df.copy()
    df_vectorized['sum'] = df_vectorized['value1'] + df_vectorized['value2'] + df_vectorized['value3']
    df_vectorized['product'] = df_vectorized['value1'] * df_vectorized['value2'] * df_vectorized['value3']
    df_vectorized['max_value'] = np.maximum.reduce([df_vectorized['value1'], df_vectorized['value2'], df_vectorized['value3']])
    
    vectorized_time = time.time() - start_time
    
    # Print results
    print(f"Loop time: {loop_time:.4f} seconds")
    print(f"Apply time: {apply_time:.4f} seconds")
    print(f"Vectorized time: {vectorized_time:.4f} seconds")
    print(f"Vectorized is {loop_time/vectorized_time:.1f}x faster than loops")
    print(f"Vectorized is {apply_time/vectorized_time:.1f}x faster than apply")
    
    return {
        'loop_time': loop_time,
        'apply_time': apply_time,
        'vectorized_time': vectorized_time
    }
```

### Explanation:
This solution demonstrates the performance benefits of vectorization by:
1. Implementing the same calculations using three different approaches:
   - Traditional Python loops with row-by-row operations (slowest)
   - Pandas apply method with lambda functions (better but still slow)
   - Vectorized operations using NumPy and Pandas (fastest)
2. Measuring and comparing execution time for each approach
3. Calculating the speedup factor of vectorization compared to other methods

The results typically show that vectorized operations are 10-100x faster than loops, which is critical for processing large datasets efficiently.

## Exercise 4: Method Chaining for Data Pipeline

```python
import pandas as pd
import numpy as np

def sales_analysis_pipeline(sales_data_path):
    """
    Create a data transformation pipeline using method chaining to analyze sales data.
    
    Parameters:
    - sales_data_path: Path to the sales data CSV file
    
    Returns:
    - DataFrame with analysis results
    """
    # Execute the data pipeline using method chaining
    result = (pd.read_csv(sales_data_path)
              # Data cleaning
              .dropna(subset=['price', 'quantity'])
              .query('price > 0 and quantity > 0')
              # Data transformation
              .assign(
                  revenue=lambda x: x['price'] * x['quantity'],
                  date=lambda x: pd.to_datetime(x['date']),
                  month=lambda x: x['date'].dt.month,
                  year=lambda x: x['date'].dt.year,
                  discount_applied=lambda x: x['discount'] > 0
              )
              # Analysis by product category and month
              .groupby(['product_category', 'year', 'month'])
              .agg({
                  'revenue': 'sum',
                  'quantity': 'sum',
                  'discount_applied': 'mean',
                  'customer_id': 'nunique'
              })
              .rename(columns={
                  'revenue': 'total_revenue',
                  'quantity': 'units_sold',
                  'discount_applied': 'discount_rate',
                  'customer_id': 'unique_customers'
              })
              # Calculate additional metrics
              .assign(
                  avg_revenue_per_customer=lambda x: x['total_revenue'] / x['unique_customers'],
                  avg_price_per_unit=lambda x: x['total_revenue'] / x['units_sold']
              )
              # Sort by revenue
              .sort_values(['year', 'month', 'total_revenue'], ascending=[True, True, False])
              .reset_index()
             )
    
    return result
```

### Explanation:
This solution creates a clean, readable data transformation pipeline using method chaining:
1. It starts by loading and cleaning the data (removing nulls and invalid values)
2. It creates derived columns using the assign method for better readability
3. It performs grouping and aggregation to calculate summary statistics
4. It renames columns to make the output more descriptive
5. It calculates additional metrics based on the aggregated data
6. It sorts the results and resets the index for a clean final output

The method chaining approach makes the code more maintainable by clearly showing the sequence of transformations without intermediate variables.

## Exercise 5: Hybrid Approach for Large Log File Analysis

```python
import pandas as pd
import numpy as np
from collections import defaultdict
import time

def analyze_user_logs(log_file_path, chunk_size=100_000):
    """
    Process a large log file using a hybrid approach combining Pandas and Python dictionaries.
    
    Parameters:
    - log_file_path: Path to the log file
    - chunk_size: Number of rows to process at once
    
    Returns:
    - Dictionary with analysis results
    """
    # Define optimal dtypes for memory efficiency
    dtypes = {
        'user_id': 'int32',
        'action': 'category',
        'page': 'category',
        'session_id': 'int32'
    }
    parse_dates = ['timestamp']
    
    # Initialize results storage using Python dictionaries for memory efficiency
    user_activity = defaultdict(int)  # Count of actions per user
    page_views = defaultdict(int)     # Count of views per page
    hourly_traffic = defaultdict(int) # Traffic by hour of day
    session_durations = {}            # Track session start/end times
    
    start_time = time.time()
    rows_processed = 0
    
    # Process the file in chunks
    for chunk_num, chunk in enumerate(pd.read_csv(log_file_path, 
                                                 dtype=dtypes, 
                                                 parse_dates=parse_dates,
                                                 chunksize=chunk_size)):
        # Track progress
        rows_processed += len(chunk)
        if chunk_num % 10 == 0:
            print(f"Processing chunk {chunk_num}, rows processed: {rows_processed}")
        
        # Extract hour information using vectorized operations
        chunk['hour'] = chunk['timestamp'].dt.hour
        
        # Update hourly traffic counts
        hour_counts = chunk.groupby('hour').size()
        for hour, count in hour_counts.items():
            hourly_traffic[hour] += count
        
        # Update page view counts
        page_counts = chunk.groupby('page').size()
        for page, count in page_counts.items():
            page_views[page] += count
        
        # Update user activity counts
        user_counts = chunk.groupby('user_id').size()
        for user, count in user_counts.items():
            user_activity[user] += count
        
        # Track session information
        for _, row in chunk.iterrows():
            user_id = row['user_id']
            session_id = row['session_id']
            timestamp = row['timestamp']
            
            # Create a unique key for each session
            session_key = f"{user_id}_{session_id}"
            
            if session_key not in session_durations:
                session_durations[session_key] = {'start': timestamp, 'end': timestamp}
            else:
                # Update session end time if this event is later
                if timestamp > session_durations[session_key]['end']:
                    session_durations[session_key]['end'] = timestamp
                # Update session start time if this event is earlier
                if timestamp < session_durations[session_key]['start']:
                    session_durations[session_key]['start'] = timestamp
        
        # Free memory
        del chunk, hour_counts, page_counts, user_counts
    
    # Calculate session duration statistics
    durations_minutes = []
    for session in session_durations.values():
        duration = (session['end'] - session['start']).total_seconds() / 60
        durations_minutes.append(duration)
    
    # Prepare results
    results = {
        'total_rows_processed': rows_processed,
        'processing_time_seconds': time.time() - start_time,
        'unique_users': len(user_activity),
        'top_pages': sorted(page_views.items(), key=lambda x: x[1], reverse=True)[:10],
        'top_users': sorted(user_activity.items(), key=lambda x: x[1], reverse=True)[:10],
        'hourly_traffic': dict(sorted(hourly_traffic.items())),
        'avg_session_duration_minutes': sum(durations_minutes) / len(durations_minutes) if durations_minutes else 0,
        'max_session_duration_minutes': max(durations_minutes) if durations_minutes else 0
    }
    
    return results
```

### Explanation:
This solution demonstrates a hybrid approach that combines the strengths of Pandas and Python dictionaries:
1. It uses Pandas for efficient data loading with optimized data types and chunking
2. It leverages Pandas vectorized operations and groupby for fast aggregations within each chunk
3. It uses Python dictionaries to store incremental results, which is more memory-efficient for large datasets
4. It processes session duration calculations using a combination of Pandas for data extraction and Python dictionaries for tracking
5. It explicitly frees memory after processing each chunk
6. It provides progress updates during processing

This hybrid approach allows processing of very large log files that wouldn't fit in memory while still leveraging Pandas' performance benefits for the operations where it excels.
