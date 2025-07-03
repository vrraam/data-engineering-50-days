import pandas as pd
import time
import os
import json
from pathlib import Path

class FormatBenchmarker:
    def __init__(self, data_file='data/ecommerce_sample.csv'):
        self.data_file = data_file
        self.results = {}
        
    def load_original_data(self):
        """Load the original CSV data"""
        print("📂 Loading original CSV data...")
        start_time = time.time()
        self.df = pd.read_csv(self.data_file)
        load_time = time.time() - start_time
        print(f"✅ Loaded {len(self.df)} records in {load_time:.2f} seconds")
        return self.df
    
    def convert_to_formats(self):
        """Convert data to different formats and measure performance"""
        formats_to_test = {
            'csv': self._save_csv,
            'json': self._save_json,
            'parquet': self._save_parquet,
            'excel': self._save_excel
        }
        
        print("\n🔄 Converting to different formats...")
        
        for format_name, save_function in formats_to_test.items():
            print(f"\n--- Converting to {format_name.upper()} ---")
            
            # Measure write time
            start_time = time.time()
            file_path = save_function()
            write_time = time.time() - start_time
            
            # Get file size
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            # Test read time
            start_time = time.time()
            self._read_file(file_path, format_name)
            read_time = time.time() - start_time
            
            # Store results
            self.results[format_name] = {
                'write_time': write_time,
                'read_time': read_time,
                'file_size_bytes': file_size,
                'file_size_mb': file_size_mb,
                'file_path': file_path
            }
            
            print(f"✅ Write time: {write_time:.3f}s")
            print(f"✅ Read time: {read_time:.3f}s")
            print(f"✅ File size: {file_size_mb:.2f} MB")
    
    def _save_csv(self):
        file_path = 'data/ecommerce_test.csv'
        self.df.to_csv(file_path, index=False)
        return file_path
    
    def _save_json(self):
        file_path = 'data/ecommerce_test.json'
        self.df.to_json(file_path, orient='records', date_format='iso')
        return file_path
    
    def _save_parquet(self):
        file_path = 'data/ecommerce_test.parquet'
        self.df.to_parquet(file_path, index=False)
        return file_path
    
    def _save_excel(self):
        file_path = 'data/ecommerce_test.xlsx'
        self.df.to_excel(file_path, index=False)
        return file_path
    
    def _read_file(self, file_path, format_name):
        """Read file based on format"""
        if format_name == 'csv':
            return pd.read_csv(file_path)
        elif format_name == 'json':
            return pd.read_json(file_path)
        elif format_name == 'parquet':
            return pd.read_parquet(file_path)
        elif format_name == 'excel':
            return pd.read_excel(file_path)
    
    def print_comparison_report(self):
        """Print a beautiful comparison report"""
        print("\n" + "="*80)
        print("🎯 FORMAT PERFORMANCE COMPARISON REPORT")
        print("="*80)
        
        # Header
        print(f"{'Format':<10} {'Write(s)':<10} {'Read(s)':<10} {'Size(MB)':<12} {'Compression':<12}")
        print("-"*70)
        
        # Calculate compression ratios (compared to CSV)
        csv_size = self.results['csv']['file_size_mb']
        
        for format_name, metrics in self.results.items():
            compression_ratio = csv_size / metrics['file_size_mb']
            print(f"{format_name:<10} {metrics['write_time']:<10.3f} {metrics['read_time']:<10.3f} "
                  f"{metrics['file_size_mb']:<12.2f} {compression_ratio:<12.2f}x")
        
        # Recommendations
        print("\n📊 PERFORMANCE INSIGHTS:")
        
        # Find best performers
        fastest_write = min(self.results.items(), key=lambda x: x[1]['write_time'])
        fastest_read = min(self.results.items(), key=lambda x: x[1]['read_time'])
        smallest_size = min(self.results.items(), key=lambda x: x[1]['file_size_mb'])
        
        print(f"🚀 Fastest Write: {fastest_write[0]} ({fastest_write[1]['write_time']:.3f}s)")
        print(f"⚡ Fastest Read: {fastest_read[0]} ({fastest_read[1]['read_time']:.3f}s)")
        print(f"💾 Smallest Size: {smallest_size[0]} ({smallest_size[1]['file_size_mb']:.2f} MB)")
        
        # Save results to JSON for later analysis
        with open('results/benchmark_results.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\n📁 Detailed results saved to: results/benchmark_results.json")

# Run the benchmark
if __name__ == "__main__":
    print("🔥 Starting Format Performance Benchmark")
    print("=" * 50)
    
    benchmarker = FormatBenchmarker()
    benchmarker.load_original_data()
    benchmarker.convert_to_formats()
    benchmarker.print_comparison_report()
    
    print("\n🎉 Benchmark completed!")
