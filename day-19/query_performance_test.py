import pandas as pd
import time
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

class QueryPerformanceTester:
    def __init__(self):
        self.file_paths = {
            'csv': 'data/ecommerce_test.csv',
            'json': 'data/ecommerce_test.json',
            'parquet': 'data/ecommerce_test.parquet',
            'excel': 'data/ecommerce_test.xlsx'
        }
        self.query_results = {}
    
    def run_query_tests(self):
        """Test different query patterns on each format"""
        print("🔍 Testing Query Performance Across Formats")
        print("=" * 60)
        
        # Define test queries
        test_queries = {
            'filter_by_region': self._test_filter_query,
            'group_by_category': self._test_aggregation_query,
            'column_selection': self._test_column_selection,
            'date_range_filter': self._test_date_filter,
            'complex_analytics': self._test_complex_query
        }
        
        for format_name, file_path in self.file_paths.items():
            print(f"\n📊 Testing {format_name.upper()} format...")
            
            # Load data once
            load_start = time.time()
            df = self._load_data(file_path, format_name)
            load_time = time.time() - load_start
            
            format_results = {'load_time': load_time}
            
            # Run each query test
            for query_name, query_function in test_queries.items():
                try:
                    query_time = query_function(df)
                    format_results[query_name] = query_time
                    print(f"  ✅ {query_name}: {query_time:.4f}s")
                except Exception as e:
                    print(f"  ❌ {query_name}: Failed ({str(e)})")
                    format_results[query_name] = None
            
            self.query_results[format_name] = format_results
    
    def _load_data(self, file_path, format_name):
        """Load data based on format"""
        if format_name == 'csv':
            return pd.read_csv(file_path)
        elif format_name == 'json':
            return pd.read_json(file_path)
        elif format_name == 'parquet':
            return pd.read_parquet(file_path)
        elif format_name == 'excel':
            return pd.read_excel(file_path)
    
    def _test_filter_query(self, df):
        """Test simple filtering performance"""
        start_time = time.time()
        result = df[df['region'] == 'North America']
        return time.time() - start_time
    
    def _test_aggregation_query(self, df):
        """Test aggregation performance"""
        start_time = time.time()
        result = df.groupby('category')['total_amount'].sum()
        return time.time() - start_time
    
    def _test_column_selection(self, df):
        """Test column selection performance"""
        start_time = time.time()
        result = df[['customer_id', 'total_amount', 'region']]
        return time.time() - start_time
    
    def _test_date_filter(self, df):
        """Test date filtering performance"""
        start_time = time.time()
        # Convert timestamp to datetime if it's not already
        if df['timestamp'].dtype == 'object':
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        result = df[df['timestamp'] >= '2024-06-01']
        return time.time() - start_time
    
    def _test_complex_query(self, df):
        """Test complex analytical query"""
        start_time = time.time()
        
        # Complex query: Monthly sales by region for electronics
        if df['timestamp'].dtype == 'object':
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        result = (df[df['category'] == 'Electronics']
                   .groupby([df['timestamp'].dt.month, 'region'])
                   ['total_amount']
                   .agg(['sum', 'mean', 'count'])
                   .reset_index())
        
        return time.time() - start_time
    
    def print_query_report(self):
        """Print comprehensive query performance report"""
        print("\n" + "="*80)
        print("🎯 QUERY PERFORMANCE ANALYSIS")
        print("="*80)
        
        # Create comparison table
        query_types = ['load_time', 'filter_by_region', 'group_by_category', 
                      'column_selection', 'date_range_filter', 'complex_analytics']
        
        print(f"{'Query Type':<20} {'CSV':<10} {'JSON':<10} {'Parquet':<10} {'Excel':<10}")
        print("-"*70)
        
        for query_type in query_types:
            row = f"{query_type:<20}"
            for format_name in ['csv', 'json', 'parquet', 'excel']:
                time_val = self.query_results[format_name].get(query_type, 'N/A')
                if time_val is not None:
                    row += f" {time_val:<9.4f}"
                else:
                    row += f" {'N/A':<9}"
            print(row)
        
        # Performance insights
        print("\n📊 KEY INSIGHTS:")
        
        # Find fastest format for each query type
        for query_type in query_types[1:]:  # Skip load_time
            times = {}
            for format_name in self.file_paths.keys():
                if (format_name in self.query_results and 
                    query_type in self.query_results[format_name] and
                    self.query_results[format_name][query_type] is not None):
                    times[format_name] = self.query_results[format_name][query_type]
            
            if times:
                fastest = min(times.items(), key=lambda x: x[1])
                slowest = max(times.items(), key=lambda x: x[1])
                speedup = slowest[1] / fastest[1]
                print(f"🚀 {query_type}: {fastest[0]} is {speedup:.1f}x faster than {slowest[0]}")
    
    def create_visualization(self):
        """Create performance visualization"""
        print("\n📊 Creating performance visualization...")
        
        # Prepare data for plotting
        formats = list(self.file_paths.keys())
        query_types = ['filter_by_region', 'group_by_category', 'column_selection']
        
        # Create subplot
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Data Format Performance Comparison', fontsize=16)
        
        # Plot 1: Query times comparison
        ax1 = axes[0, 0]
        for query_type in query_types:
            times = []
            for format_name in formats:
                time_val = self.query_results[format_name].get(query_type, 0)
                times.append(time_val if time_val is not None else 0)
            
            ax1.plot(formats, times, marker='o', label=query_type)
        
        ax1.set_title('Query Performance by Format')
        ax1.set_ylabel('Time (seconds)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Load times
        ax2 = axes[0, 1]
        load_times = [self.query_results[fmt]['load_time'] for fmt in formats]
        bars = ax2.bar(formats, load_times, color=['skyblue', 'lightgreen', 'coral', 'gold'])
        ax2.set_title('Data Loading Performance')
        ax2.set_ylabel('Time (seconds)')
        
        # Add value labels on bars
        for bar, time_val in zip(bars, load_times):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                    f'{time_val:.3f}s', ha='center', va='bottom')
        
        # Plot 3: Relative performance (normalized)
        ax3 = axes[1, 0]
        baseline_format = 'csv'
        
        relative_performance = {}
        for query_type in query_types:
            relative_performance[query_type] = []
            baseline_time = self.query_results[baseline_format][query_type]
            
            for format_name in formats:
                current_time = self.query_results[format_name][query_type]
                if current_time is not None and baseline_time is not None:
                    ratio = baseline_time / current_time  # Higher is better
                    relative_performance[query_type].append(ratio)
                else:
                    relative_performance[query_type].append(0)
        
        x = range(len(formats))
        width = 0.25
        
        for i, query_type in enumerate(query_types):
            ax3.bar([xi + i*width for xi in x], relative_performance[query_type], 
                   width, label=query_type)
        
        ax3.set_title('Relative Performance (vs CSV baseline)')
        ax3.set_ylabel('Speedup Factor')
        ax3.set_xticks([xi + width for xi in x])
        ax3.set_xticklabels(formats)
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Summary heatmap
        ax4 = axes[1, 1]
        
        # Create heatmap data
        heatmap_data = []
        for query_type in query_types:
            row = []
            for format_name in formats:
                time_val = self.query_results[format_name].get(query_type, 0)
                row.append(time_val if time_val is not None else 0)
            heatmap_data.append(row)
        
        sns.heatmap(heatmap_data, annot=True, fmt='.4f', 
                   xticklabels=formats, yticklabels=query_types,
                   cmap='RdYlBu_r', ax=ax4)
        ax4.set_title('Performance Heatmap (Lower is Better)')
        
        plt.tight_layout()
        plt.savefig('results/performance_comparison.png', dpi=300, bbox_inches='tight')
        print("✅ Visualization saved to: results/performance_comparison.png")
        
        # Show the plot
        plt.show()

# Run the query performance test
if __name__ == "__main__":
    print("🔥 Starting Advanced Query Performance Testing")
    print("=" * 60)
    
    tester = QueryPerformanceTester()
    tester.run_query_tests()
    tester.print_query_report()
    tester.create_visualization()
    
    print("\n🎉 Query performance testing completed!")
    print("📊 Check the generated chart to see visual performance differences!")
