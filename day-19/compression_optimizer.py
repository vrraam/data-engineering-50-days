import pandas as pd
import time
import os
import json
import gzip
import bz2
from pathlib import Path

class CompressionOptimizer:
    def __init__(self, data_file='data/ecommerce_sample.csv'):
        self.data_file = data_file
        self.df = None
        self.compression_results = {}
        
    def load_data(self):
        """Load the original data"""
        print("📂 Loading data for compression testing...")
        self.df = pd.read_csv(self.data_file)
        print(f"✅ Loaded {len(self.df)} records")
        
    def test_compression_algorithms(self):
        """Test different compression algorithms with various formats"""
        print("\n🗜️ Testing Compression Algorithms")
        print("=" * 50)
        
        # Test combinations of formats and compression
        test_combinations = [
            # CSV with different compression
            ('csv', None, 'No compression'),
            ('csv', 'gzip', 'GZIP compression'),
            ('csv', 'bz2', 'BZ2 compression'),
            
            # Parquet with different compression
            ('parquet', 'snappy', 'Snappy (default)'),
            ('parquet', 'gzip', 'GZIP'),
            ('parquet', 'lz4', 'LZ4'),
            ('parquet', 'brotli', 'Brotli'),
            
            # JSON with compression
            ('json', None, 'No compression'),
            ('json', 'gzip', 'GZIP compression'),
        ]
        
        for format_type, compression, description in test_combinations:
            print(f"\n📊 Testing {format_type.upper()} with {description}")
            result = self._test_compression_combo(format_type, compression)
            
            key = f"{format_type}_{compression if compression else 'none'}"
            self.compression_results[key] = result
            
            print(f"  ✅ Write time: {result['write_time']:.3f}s")
            print(f"  ✅ File size: {result['file_size_mb']:.2f} MB")
            print(f"  ✅ Compression ratio: {result['compression_ratio']:.2f}x")
            print(f"  ✅ Read time: {result['read_time']:.3f}s")
    
    def _test_compression_combo(self, format_type, compression):
        """Test a specific format-compression combination"""
        filename = f"data/test_{format_type}_{compression if compression else 'none'}"
        
        # Write with compression
        start_time = time.time()
        
        if format_type == 'csv':
            if compression == 'gzip':
                filename += '.csv.gz'
                with gzip.open(filename, 'wt') as f:
                    self.df.to_csv(f, index=False)
            elif compression == 'bz2':
                filename += '.csv.bz2'
                with bz2.open(filename, 'wt') as f:
                    self.df.to_csv(f, index=False)
            else:
                filename += '.csv'
                self.df.to_csv(filename, index=False)
                
        elif format_type == 'parquet':
            filename += '.parquet'
            if compression:
                self.df.to_parquet(filename, compression=compression)
            else:
                self.df.to_parquet(filename)
                
        elif format_type == 'json':
            if compression == 'gzip':
                filename += '.json.gz'
                with gzip.open(filename, 'wt') as f:
                    self.df.to_json(f, orient='records')
            else:
                filename += '.json'
                self.df.to_json(filename, orient='records')
        
        write_time = time.time() - start_time
        
        # Get file size
        file_size = os.path.getsize(filename)
        file_size_mb = file_size / (1024 * 1024)
        
        # Calculate compression ratio (vs uncompressed CSV)
        if not hasattr(self, 'baseline_size'):
            # Create baseline uncompressed CSV
            self.df.to_csv('data/baseline.csv', index=False)
            self.baseline_size = os.path.getsize('data/baseline.csv')
        
        compression_ratio = self.baseline_size / file_size
        
        # Test read time
        start_time = time.time()
        try:
            if format_type == 'csv':
                if compression == 'gzip':
                    with gzip.open(filename, 'rt') as f:
                        pd.read_csv(f)
                elif compression == 'bz2':
                    with bz2.open(filename, 'rt') as f:
                        pd.read_csv(f)
                else:
                    pd.read_csv(filename)
            elif format_type == 'parquet':
                pd.read_parquet(filename)
            elif format_type == 'json':
                if compression == 'gzip':
                    with gzip.open(filename, 'rt') as f:
                        pd.read_json(f)
                else:
                    pd.read_json(filename)
        except Exception as e:
            print(f"    ⚠️  Read test failed: {e}")
        
        read_time = time.time() - start_time
        
        return {
            'write_time': write_time,
            'read_time': read_time,
            'file_size_mb': file_size_mb,
            'compression_ratio': compression_ratio,
            'filename': filename
        }
    
    def print_compression_report(self):
        """Print comprehensive compression analysis"""
        print("\n" + "="*80)
        print("🎯 COMPRESSION ANALYSIS REPORT")
        print("="*80)
        
        # Sort by compression ratio
        sorted_results = sorted(self.compression_results.items(), 
                              key=lambda x: x[1]['compression_ratio'], reverse=True)
        
        print(f"{'Format + Compression':<25} {'Write(s)':<10} {'Read(s)':<10} {'Size(MB)':<10} {'Ratio':<10}")
        print("-"*75)
        
        for combo_name, result in sorted_results:
            print(f"{combo_name:<25} {result['write_time']:<10.3f} {result['read_time']:<10.3f} "
                  f"{result['file_size_mb']:<10.2f} {result['compression_ratio']:<10.2f}x")
        
        # Best performers
        print("\n📊 COMPRESSION CHAMPIONS:")
        
        best_compression = max(sorted_results, key=lambda x: x[1]['compression_ratio'])
        fastest_write = min(sorted_results, key=lambda x: x[1]['write_time'])
        fastest_read = min(sorted_results, key=lambda x: x[1]['read_time'])
        
        print(f"🏆 Best Compression: {best_compression[0]} ({best_compression[1]['compression_ratio']:.2f}x)")
        print(f"🚀 Fastest Write: {fastest_write[0]} ({fastest_write[1]['write_time']:.3f}s)")
        print(f"⚡ Fastest Read: {fastest_read[0]} ({fastest_read[1]['read_time']:.3f}s)")

class SmartFormatSelector:
    """Intelligent format selection based on use case"""
    
    def __init__(self):
        self.decision_matrix = {
            'data_size': {'small': 0, 'medium': 1, 'large': 2},
            'query_pattern': {'oltp': 0, 'mixed': 1, 'analytics': 2},
            'schema_evolution': {'never': 0, 'rare': 1, 'frequent': 2},
            'human_readable': {'required': 2, 'nice': 1, 'not_needed': 0},
            'storage_cost': {'low_priority': 0, 'medium': 1, 'critical': 2}
        }
        
        self.format_scores = {
            'csv': {'base_score': 5, 'strengths': ['human_readable', 'universal'], 'weaknesses': ['large_files', 'no_schema']},
            'json': {'base_score': 6, 'strengths': ['flexible', 'human_readable'], 'weaknesses': ['verbose', 'slower']},
            'parquet': {'base_score': 8, 'strengths': ['analytics', 'compression'], 'weaknesses': ['binary', 'complexity']},
            'avro': {'base_score': 7, 'strengths': ['schema_evolution', 'streaming'], 'weaknesses': ['binary', 'tooling']}
        }
    
    def recommend_format(self, requirements):
        """Recommend optimal format based on requirements"""
        print("\n🎯 SMART FORMAT RECOMMENDATION")
        print("=" * 50)
        print("📋 Your requirements:")
        for key, value in requirements.items():
            print(f"  • {key}: {value}")
        
        scores = {}
        
        for format_name, format_info in self.format_scores.items():
            score = format_info['base_score']
            reasoning = []
            
            # Apply scoring rules
            if requirements['data_size'] == 'large':
                if format_name in ['parquet', 'avro']:
                    score += 3
                    reasoning.append("✅ Excellent for large datasets")
                else:
                    score -= 2
                    reasoning.append("❌ Not optimal for large datasets")
            
            if requirements['query_pattern'] == 'analytics':
                if format_name == 'parquet':
                    score += 4
                    reasoning.append("✅ Perfect for analytical queries")
                elif format_name in ['csv', 'json']:
                    score -= 3
                    reasoning.append("❌ Poor for analytical workloads")
            
            if requirements['schema_evolution'] == 'frequent':
                if format_name == 'avro':
                    score += 3
                    reasoning.append("✅ Excellent schema evolution support")
                else:
                    score -= 1
                    reasoning.append("⚠️ Limited schema evolution")
            
            if requirements['human_readable'] == 'required':
                if format_name in ['csv', 'json']:
                    score += 2
                    reasoning.append("✅ Human readable format")
                else:
                    score -= 3
                    reasoning.append("❌ Binary format, not human readable")
            
            if requirements['storage_cost'] == 'critical':
                if format_name in ['parquet', 'avro']:
                    score += 3
                    reasoning.append("✅ Excellent compression")
                else:
                    score -= 2
                    reasoning.append("❌ Poor compression")
            
            scores[format_name] = {'score': score, 'reasoning': reasoning}
        
        # Sort by score
        ranked_formats = sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print(f"\n🏆 RECOMMENDATIONS (Ranked by suitability):")
        for i, (format_name, info) in enumerate(ranked_formats):
            print(f"\n{i+1}. {format_name.upper()} (Score: {info['score']}/15)")
            for reason in info['reasoning']:
                print(f"   {reason}")
        
        best_format = ranked_formats[0][0]
        print(f"\n🎯 RECOMMENDED FORMAT: {best_format.upper()}")
        return best_format

# Example usage and testing
if __name__ == "__main__":
    print("🔥 Starting Compression Optimization & Format Selection")
    print("=" * 60)
    
    # Test compression
    optimizer = CompressionOptimizer()
    optimizer.load_data()
    optimizer.test_compression_algorithms()
    optimizer.print_compression_report()
    
    # Test format selection
    print("\n" + "="*60)
    selector = SmartFormatSelector()
    
    # Example scenarios
    scenarios = [
        {
            'name': 'Large Analytics Platform',
            'requirements': {
                'data_size': 'large',
                'query_pattern': 'analytics',
                'schema_evolution': 'rare',
                'human_readable': 'not_needed',
                'storage_cost': 'critical'
            }
        },
        {
            'name': 'Data Exchange with Partners',
            'requirements': {
                'data_size': 'medium',
                'query_pattern': 'mixed',
                'schema_evolution': 'never',
                'human_readable': 'required',
                'storage_cost': 'low_priority'
            }
        },
        {
            'name': 'Real-time Streaming Pipeline',
            'requirements': {
                'data_size': 'medium',
                'query_pattern': 'oltp',
                'schema_evolution': 'frequent',
                'human_readable': 'not_needed',
                'storage_cost': 'medium'
            }
        }
    ]
    
    for scenario in scenarios:
        print(f"\n📊 SCENARIO: {scenario['name']}")
        recommended = selector.recommend_format(scenario['requirements'])
        print(f"💡 Use Case: {scenario['name']} → {recommended.upper()}")
