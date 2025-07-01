import pandas as pd
import numpy as np
import time
import psutil
import gc
from functools import wraps

# Load data with correct delimiter
print("=== LOADING DATASET ===")
# Try tab-separated first
df = pd.read_csv('marketing_campaign.csv', sep='\t')

print(f"Dataset loaded successfully!")
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns[:5])}...")  # Show first 5 columns
print(f"Total columns: {len(df.columns)}")
print(f"\nFirst few rows:")
print(df.head(3))


# === STEP 2: MEMORY ANALYSIS ===
print("\n" + "="*50)
print("=== MEMORY ANALYSIS ===")
print("="*50)

# Check memory usage
print(f"DataFrame shape: {df.shape}")
print("\nMemory usage by column:")
memory_usage = df.memory_usage(deep=True)
print(memory_usage)

print(f"\nTotal memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Detailed info about data types
print("\n=== DATA TYPES ===")
print(df.dtypes)

print(f"\nDataFrame info:")
df.info(memory_usage='deep')

# Let's see what types of data we have
print(f"\nData type distribution:")
dtype_counts = df.dtypes.value_counts()
for dtype, count in dtype_counts.items():
    print(f"{dtype}: {count} columns")



# === STEP 3: NUMERIC DATA TYPE OPTIMIZATION ===
print("\n" + "="*50)
print("=== OPTIMIZING NUMERIC TYPES ===")
print("="*50)

def optimize_numeric_dtypes(df):
    """
    Optimize numeric columns to use smallest possible data types
    """
    print("Optimizing numeric columns...")
    optimized_df = df.copy()
    
    # Optimize integer columns
    for col in df.select_dtypes(include=['int64']).columns:
        original_memory = df[col].memory_usage(deep=True)
        
        # Check the range of values to choose the best integer type
        col_min = df[col].min()
        col_max = df[col].max()
        
        print(f"\n{col}: range [{col_min} to {col_max}]")
        
        # Downcast to smallest integer type
        optimized_df[col] = pd.to_numeric(df[col], downcast='integer')
        
        new_memory = optimized_df[col].memory_usage(deep=True)
        reduction = (1 - new_memory/original_memory) * 100
        
        print(f"  {df[col].dtype} → {optimized_df[col].dtype} "
              f"({reduction:.1f}% memory reduction)")
    
    # Optimize float columns
    for col in df.select_dtypes(include=['float64']).columns:
        original_memory = df[col].memory_usage(deep=True)
        
        # Downcast to smallest float type
        optimized_df[col] = pd.to_numeric(df[col], downcast='float')
        
        new_memory = optimized_df[col].memory_usage(deep=True)
        reduction = (1 - new_memory/original_memory) * 100
        
        print(f"\n{col}: {df[col].dtype} → {optimized_df[col].dtype} "
              f"({reduction:.1f}% memory reduction)")
    
    return optimized_df

# Apply optimization
df_optimized = optimize_numeric_dtypes(df)

# Compare memory usage
print(f"\n=== MEMORY COMPARISON ===")
original_memory = df.memory_usage(deep=True).sum()
optimized_memory = df_optimized.memory_usage(deep=True).sum()
reduction = (1 - optimized_memory/original_memory) * 100

print(f"Original memory usage: {original_memory / 1024:.1f} KB")
print(f"Optimized memory usage: {optimized_memory / 1024:.1f} KB")
print(f"Memory reduction: {reduction:.1f}%")


# === STEP 4: CATEGORICAL DATA TYPE OPTIMIZATION ===
print("\n" + "="*50)
print("=== OPTIMIZING CATEGORICAL TYPES ===")
print("="*50)

def optimize_categorical_dtypes(df):
    """
    Convert string columns with low cardinality to categorical
    """
    print("Analyzing object columns for categorical optimization...")
    optimized_df = df.copy()
    
    # Identify categorical candidates (low cardinality string columns)
    for col in df.select_dtypes(include=['object']).columns:
        num_unique = df[col].nunique()
        total_count = len(df[col])
        uniqueness_ratio = num_unique / total_count
        
        print(f"\n{col}:")
        print(f"  Unique values: {num_unique} out of {total_count}")
        print(f"  Uniqueness ratio: {uniqueness_ratio:.3f}")
        
        # If less than 50% unique values, convert to categorical
        if uniqueness_ratio < 0.5:
            original_memory = df[col].memory_usage(deep=True)
            
            # Convert to categorical
            optimized_df[col] = df[col].astype('category')
            
            new_memory = optimized_df[col].memory_usage(deep=True)
            reduction = (1 - new_memory/original_memory) * 100
            
            print(f"  ✅ Converted to categorical ({reduction:.1f}% memory reduction)")
            
            # Show the categories
            print(f"  Categories: {list(optimized_df[col].cat.categories)}")
        else:
            print(f"  ❌ Keeping as object (too many unique values)")
    
    return optimized_df

# Apply categorical optimization
df_optimized = optimize_categorical_dtypes(df_optimized)

# Final memory comparison
print(f"\n" + "="*50)
print("=== FINAL MEMORY OPTIMIZATION RESULTS ===")
print("="*50)

original_memory = df.memory_usage(deep=True).sum()
final_optimized_memory = df_optimized.memory_usage(deep=True).sum()
total_reduction = (1 - final_optimized_memory/original_memory) * 100

print(f"Original memory usage: {original_memory / 1024:.1f} KB")
print(f"Final optimized memory usage: {final_optimized_memory / 1024:.1f} KB")
print(f"TOTAL memory reduction: {total_reduction:.1f}% 🚀")

print(f"\nOptimized data types:")
print(df_optimized.dtypes.value_counts())


# === STEP 5: VECTORIZATION PERFORMANCE COMPARISON ===
print("\n" + "="*60)
print("=== VECTORIZATION: SPEED REVOLUTION ===")
print("="*60)

def demonstrate_vectorization(df):
    """
    Compare loop vs vectorized operations performance
    """
    print("Calculating Customer Lifetime Value (CLV)...")
    print("CLV = Average Purchase Value × Purchase Frequency × 2 years")
    
    # Method 1: Loop-based approach (SLOW - Don't do this!)
    print("\n🐌 Method 1: Loop-based approach (SLOW)")
    start_time = time.time()
    
    clv_loop = []
    product_cols = ['MntWines', 'MntFruits', 'MntMeatProducts', 
                   'MntFishProducts', 'MntSweetProducts', 'MntGoldProds']
    purchase_cols = ['NumWebPurchases', 'NumCatalogPurchases', 'NumStorePurchases']
    
    for i in range(len(df)):
        # Calculate average purchase value
        avg_purchase = sum(df.iloc[i][col] for col in product_cols) / len(product_cols)
        
        # Calculate purchase frequency  
        frequency = sum(df.iloc[i][col] for col in purchase_cols)
        
        # Calculate CLV
        clv_loop.append(avg_purchase * frequency * 2)
    
    loop_time = time.time() - start_time
    print(f"   Time taken: {loop_time:.4f} seconds")
    
    # Method 2: Vectorized approach (FAST - Always do this!)
    print("\n🚀 Method 2: Vectorized approach (FAST)")
    start_time = time.time()
    
    # Calculate average purchase value across all product categories
    avg_purchase_vectorized = df[product_cols].mean(axis=1)
    
    # Calculate purchase frequency
    frequency_vectorized = df[purchase_cols].sum(axis=1)
    
    # Calculate CLV
    clv_vectorized = avg_purchase_vectorized * frequency_vectorized * 2
    
    vectorized_time = time.time() - start_time
    print(f"   Time taken: {vectorized_time:.4f} seconds")
    
    # Performance improvement
    if vectorized_time > 0:
        speedup = loop_time / vectorized_time
        print(f"\n⚡ SPEEDUP: {speedup:.1f}x faster with vectorization!")
    
    return clv_vectorized

# Apply vectorization demo
print("Testing with our optimized dataset...")
clv_values = demonstrate_vectorization(df_optimized)

print(f"\nCLV calculation completed!")
print(f"Sample CLV values: {clv_values.head().values}")
print(f"Average CLV: ${clv_values.mean():.2f}")


# === STEP 6: ADVANCED VECTORIZATION TECHNIQUES ===
print("\n" + "="*60)
print("=== ADVANCED VECTORIZATION TECHNIQUES ===")
print("="*60)

def advanced_vectorization_techniques(df):
    """
    Demonstrate advanced vectorization patterns used in production
    """
    df_advanced = df.copy()
    
    # Technique 1: Conditional operations with np.where (super fast if-else)
    print("1️⃣ Conditional Customer Segmentation with np.where")
    start_time = time.time()
    
    # Create customer segments based on income and spending
    spending_cols = ['MntWines', 'MntFruits', 'MntMeatProducts', 
                    'MntFishProducts', 'MntSweetProducts', 'MntGoldProds']
    total_spending = df_advanced[spending_cols].sum(axis=1)
    
    df_advanced['customer_segment'] = np.where(
        df_advanced['Income'] > 75000,
        np.where(total_spending > 1000, 'Premium', 'High-Income'),
        np.where(df_advanced['Income'] > 40000, 'Middle-Income', 'Budget')
    )
    
    segment_time = time.time() - start_time
    print(f"   ✅ Segmentation completed in {segment_time:.4f} seconds")
    print(f"   📊 Segments: {df_advanced['customer_segment'].value_counts().to_dict()}")
    
    # Technique 2: Age groups with pd.cut (binning made easy)
    print(f"\n2️⃣ Age Grouping with pd.cut")
    start_time = time.time()
    
    current_year = 2024
    df_advanced['age'] = current_year - df_advanced['Year_Birth']
    df_advanced['age_group'] = pd.cut(df_advanced['age'],
                                     bins=[0, 30, 45, 60, 100],
                                     labels=['Young', 'Middle-Aged', 'Senior', 'Elder'])
    
    age_time = time.time() - start_time
    print(f"   ✅ Age grouping completed in {age_time:.4f} seconds")
    print(f"   📊 Age groups: {df_advanced['age_group'].value_counts().to_dict()}")
    
    # Technique 3: Rolling calculations (time series magic)
    print(f"\n3️⃣ Rolling Window Calculations")
    start_time = time.time()
    
    # Sort by a column for rolling calculations (simulate time-based data)
    df_sorted = df_advanced.sort_values('Income')
    
    # Calculate rolling average income (useful for trend analysis)
    window_size = 100
    df_sorted['rolling_income_avg'] = df_sorted['Income'].rolling(
        window=window_size, min_periods=1
    ).mean()
    
    rolling_time = time.time() - start_time
    print(f"   ✅ Rolling calculation completed in {rolling_time:.4f} seconds")
    print(f"   📈 Rolling window size: {window_size} customers")
    
    # Technique 4: String operations (vectorized text processing)
    print(f"\n4️⃣ Vectorized String Operations")
    start_time = time.time()
    
    # Convert education to uppercase and create education level scores
    df_advanced['education_upper'] = df_advanced['Education'].str.upper()
    
    # Create education score mapping
    education_scores = {
        'BASIC': 1, '2N CYCLE': 2, 'GRADUATION': 3, 
        'MASTER': 4, 'PHD': 5
    }
    df_advanced['education_score'] = df_advanced['education_upper'].map(education_scores)
    
    string_time = time.time() - start_time
    print(f"   ✅ String operations completed in {string_time:.4f} seconds")
    print(f"   🎓 Education scores: {df_advanced['education_score'].value_counts().sort_index().to_dict()}")
    
    return df_advanced

# Apply advanced techniques
df_advanced = advanced_vectorization_techniques(df_optimized)

print(f"\n🎯 Advanced vectorization complete!")
print(f"New columns added: {list(df_advanced.columns[-6:])}")  # Show last 6 columns
print(f"Final dataset shape: {df_advanced.shape}")


# === STEP 7: PERFORMANCE PROFILING & MONITORING ===
print("\n" + "="*60)
print("=== PERFORMANCE PROFILING & MONITORING ===")
print("="*60)

class PandasPerformanceProfiler:
    """
    Professional performance profiler for pandas operations
    """
    def __init__(self):
        self.profiles = []
        print("🔍 Performance Profiler initialized")
    
    def profile_operation(self, operation_name):
        """Decorator to profile pandas operations"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Pre-operation metrics
                process = psutil.Process()
                start_memory = process.memory_info().rss / 1024 / 1024  # MB
                start_time = time.time()
                
                # Execute operation
                result = func(*args, **kwargs)
                
                # Post-operation metrics
                end_time = time.time()
                end_memory = process.memory_info().rss / 1024 / 1024  # MB
                
                # Calculate metrics
                execution_time = end_time - start_time
                memory_delta = end_memory - start_memory
                
                # Store profile
                profile = {
                    'operation': operation_name,
                    'execution_time': execution_time,
                    'memory_before_mb': start_memory,
                    'memory_after_mb': end_memory,
                    'memory_delta_mb': memory_delta,
                    'timestamp': time.time()
                }
                self.profiles.append(profile)
                
                print(f"📊 [PROFILE] {operation_name}: {execution_time:.4f}s, "
                      f"Memory: {memory_delta:+.2f}MB")
                
                return result
            return wrapper
        return decorator
    
    def get_performance_summary(self):
        """Get summary of all profiled operations"""
        if not self.profiles:
            return "No operations profiled yet"
        
        df_profiles = pd.DataFrame(self.profiles)
        
        summary = {
            'total_operations': len(df_profiles),
            'total_time': df_profiles['execution_time'].sum(),
            'avg_time_per_operation': df_profiles['execution_time'].mean(),
            'max_memory_usage': df_profiles['memory_after_mb'].max(),
            'total_memory_allocated': df_profiles['memory_delta_mb'].sum(),
            'slowest_operation': df_profiles.loc[df_profiles['execution_time'].idxmax(), 'operation'],
            'fastest_operation': df_profiles.loc[df_profiles['execution_time'].idxmin(), 'operation']
        }
        
        return summary, df_profiles

# Initialize profiler
profiler = PandasPerformanceProfiler()

# Example: Profile different operations
@profiler.profile_operation("Data Loading")
def load_data():
    return pd.read_csv('marketing_campaign.csv', sep=None, engine='python')

@profiler.profile_operation("Memory Optimization")
def optimize_memory(df):
    df_opt = optimize_numeric_dtypes(df)
    df_opt = optimize_categorical_dtypes(df_opt)
    return df_opt

@profiler.profile_operation("Complex Analysis")
def perform_analysis(df):
    # Group analysis
    education_stats = df.groupby('Education').agg({
        'Income': ['mean', 'std'],
        'age': 'mean'
    })
    
    # Pivot analysis
    segment_pivot = df.pivot_table(
        values='Income',
        index='Education',
        columns='customer_segment',
        aggfunc='mean'
    )
    
    return education_stats, segment_pivot

@profiler.profile_operation("Vectorized Calculations")
def vectorized_operations(df):
    # Multiple vectorized calculations
    df['income_per_age'] = df['Income'] / df['age']
    df['spending_efficiency'] = df[['MntWines', 'MntFruits', 'MntMeatProducts', 
                                   'MntFishProducts', 'MntSweetProducts', 
                                   'MntGoldProds']].sum(axis=1) / df['Income']
    return df

# Run profiled operations
print("\n🚀 Running profiled operations...")
raw_data = load_data()
optimized_data = optimize_memory(raw_data)
analysis_results = perform_analysis(df_advanced)
enhanced_data = vectorized_operations(df_advanced.copy())

# Get performance summary
summary, profile_df = profiler.get_performance_summary()

print(f"\n" + "="*50)
print("=== PERFORMANCE SUMMARY ===")
print("="*50)
for key, value in summary.items():
    if 'time' in key and key != 'timestamp':
        print(f"{key.replace('_', ' ').title()}: {value:.4f} seconds")
    elif 'memory' in key:
        print(f"{key.replace('_', ' ').title()}: {value:.2f} MB")
    else:
        print(f"{key.replace('_', ' ').title()}: {value}")

print(f"\n📈 Performance Profile Table:")
print(profile_df[['operation', 'execution_time', 'memory_delta_mb']].round(4))
