# Data Pipeline Testing Strategies Implementation
# Save this as: notebooks/04_testing_strategies.py

import pandas as pd
import numpy as np
import unittest
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

class DataTransformationFunctions:
    """Data transformation functions to be tested"""
    
    @staticmethod
    def clean_title(title):
        """Clean and standardize title format"""
        if pd.isna(title):
            return None
        return str(title).strip().title()
    
    @staticmethod
    def standardize_country(country):
        """Standardize country names"""
        if pd.isna(country):
            return None
        
        # Handle multiple countries
        if ',' in str(country):
            countries = [c.strip() for c in str(country).split(',')]
            return ', '.join(sorted(countries))
        
        return str(country).strip()
    
    @staticmethod
    def validate_release_year(year):
        """Validate and correct release year"""
        if pd.isna(year):
            return None
        
        try:
            year = int(year)
            current_year = datetime.now().year
            
            if 1900 <= year <= current_year:
                return year
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def calculate_content_age(release_year):
        """Calculate content age in years"""
        if pd.isna(release_year):
            return None
        
        try:
            current_year = datetime.now().year
            age = current_year - int(release_year)
            return max(0, age)  # Age cannot be negative
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def extract_duration_minutes(duration, content_type):
        """Extract duration in minutes from duration string"""
        if pd.isna(duration) or pd.isna(content_type):
            return None
        
        duration_str = str(duration).strip()
        
        if content_type == 'Movie':
            # Extract minutes from "X min" format
            if 'min' in duration_str:
                try:
                    minutes = int(duration_str.replace('min', '').strip())
                    return minutes if minutes > 0 else None
                except ValueError:
                    return None
        
        elif content_type == 'TV Show':
            # For TV shows, return None (seasons, not minutes)
            return None
        
        return None

# Unit Tests for Data Transformation Functions
class TestDataTransformationFunctions(unittest.TestCase):
    """Unit tests for data transformation functions"""
    
    def test_clean_title(self):
        """Test title cleaning function"""
        # Test normal cases
        self.assertEqual(DataTransformationFunctions.clean_title("the matrix"), "The Matrix")
        self.assertEqual(DataTransformationFunctions.clean_title("  AVATAR  "), "Avatar")
        self.assertEqual(DataTransformationFunctions.clean_title("lord of the rings"), "Lord Of The Rings")
        
        # Test edge cases
        self.assertIsNone(DataTransformationFunctions.clean_title(None))
        self.assertIsNone(DataTransformationFunctions.clean_title(np.nan))
        self.assertEqual(DataTransformationFunctions.clean_title(""), "")
        
        # Test special characters
        self.assertEqual(DataTransformationFunctions.clean_title("marvel's iron man"), "Marvel'S Iron Man")
    
    def test_standardize_country(self):
        """Test country standardization function"""
        # Test single country
        self.assertEqual(DataTransformationFunctions.standardize_country("United States"), "United States")
        self.assertEqual(DataTransformationFunctions.standardize_country("  india  "), "india")
        
        # Test multiple countries
        self.assertEqual(DataTransformationFunctions.standardize_country("United States, Canada"), "Canada, United States")
        self.assertEqual(DataTransformationFunctions.standardize_country("India, United Kingdom, Canada"), "Canada, India, United Kingdom")
        
        # Test edge cases
        self.assertIsNone(DataTransformationFunctions.standardize_country(None))
        self.assertIsNone(DataTransformationFunctions.standardize_country(np.nan))
    
    def test_validate_release_year(self):
        """Test release year validation function"""
        # Test valid years
        self.assertEqual(DataTransformationFunctions.validate_release_year(2020), 2020)
        self.assertEqual(DataTransformationFunctions.validate_release_year(1950), 1950)
        self.assertEqual(DataTransformationFunctions.validate_release_year("2010"), 2010)
        
        # Test invalid years
        self.assertIsNone(DataTransformationFunctions.validate_release_year(1800))  # Too old
        self.assertIsNone(DataTransformationFunctions.validate_release_year(2030))  # Future
        self.assertIsNone(DataTransformationFunctions.validate_release_year("invalid"))
        self.assertIsNone(DataTransformationFunctions.validate_release_year(None))
        self.assertIsNone(DataTransformationFunctions.validate_release_year(np.nan))
    
    def test_calculate_content_age(self):
        """Test content age calculation function"""
        current_year = datetime.now().year
        
        # Test normal cases
        self.assertEqual(DataTransformationFunctions.calculate_content_age(2020), current_year - 2020)
        self.assertEqual(DataTransformationFunctions.calculate_content_age(current_year), 0)
        
        # Test edge cases
        self.assertIsNone(DataTransformationFunctions.calculate_content_age(None))
        self.assertIsNone(DataTransformationFunctions.calculate_content_age(np.nan))
        self.assertIsNone(DataTransformationFunctions.calculate_content_age("invalid"))
        
        # Test boundary cases
        self.assertEqual(DataTransformationFunctions.calculate_content_age(current_year + 1), 0)  # Future year should return 0
    
    def test_extract_duration_minutes(self):
        """Test duration extraction function"""
        # Test movie durations
        self.assertEqual(DataTransformationFunctions.extract_duration_minutes("120 min", "Movie"), 120)
        self.assertEqual(DataTransformationFunctions.extract_duration_minutes("90 min", "Movie"), 90)
        self.assertEqual(DataTransformationFunctions.extract_duration_minutes("0 min", "Movie"), None)  # Invalid duration
        
        # Test TV show durations (should return None)
        self.assertIsNone(DataTransformationFunctions.extract_duration_minutes("2 Seasons", "TV Show"))
        self.assertIsNone(DataTransformationFunctions.extract_duration_minutes("1 Season", "TV Show"))
        
        # Test edge cases
        self.assertIsNone(DataTransformationFunctions.extract_duration_minutes(None, "Movie"))
        self.assertIsNone(DataTransformationFunctions.extract_duration_minutes("120 min", None))
        self.assertIsNone(DataTransformationFunctions.extract_duration_minutes("invalid format", "Movie"))

class DataPipelineIntegrationTester:
    """Integration testing for data pipeline components"""
    
    def __init__(self, data_path='data/netflix_titles.csv'):
        self.data_path = data_path
        self.df = None
        self.processed_df = None
    
    def load_test_data(self):
        """Load test data"""
        self.df = pd.read_csv(self.data_path)
        print(f"✅ Loaded test dataset: {self.df.shape}")
    
    def test_data_ingestion(self):
        """Test data ingestion process"""
        test_results = {}
        
        # Test 1: File exists and is readable
        try:
            self.load_test_data()
            test_results['file_readable'] = True
        except Exception as e:
            test_results['file_readable'] = False
            test_results['file_error'] = str(e)
        
        # Test 2: Expected columns exist
        expected_columns = [
            'show_id', 'type', 'title', 'director', 'cast',
            'country', 'date_added', 'release_year', 'rating',
            'duration', 'listed_in', 'description'
        ]
        
        missing_columns = set(expected_columns) - set(self.df.columns)
        test_results['schema_valid'] = len(missing_columns) == 0
        test_results['missing_columns'] = list(missing_columns)
        
        # Test 3: Minimum data volume
        min_expected_rows = 1000
        test_results['sufficient_data'] = len(self.df) >= min_expected_rows
        test_results['actual_rows'] = len(self.df)
        
        # Test 4: Data types are reasonable
        numeric_columns = ['release_year']
        for col in numeric_columns:
            if col in self.df.columns:
                try:
                    pd.to_numeric(self.df[col], errors='coerce')
                    test_results[f'{col}_numeric_convertible'] = True
                except:
                    test_results[f'{col}_numeric_convertible'] = False
        
        return test_results
    
    def test_data_transformation(self):
        """Test data transformation pipeline"""
        if self.df is None:
            self.load_test_data()
        
        test_results = {}
        
        # Apply transformations
        transformed_df = self.df.copy()
        
        # Test title cleaning
        transformed_df['title_cleaned'] = transformed_df['title'].apply(
            DataTransformationFunctions.clean_title
        )
        
        # Test country standardization
        transformed_df['country_standardized'] = transformed_df['country'].apply(
            DataTransformationFunctions.standardize_country
        )
        
        # Test release year validation
        transformed_df['release_year_validated'] = transformed_df['release_year'].apply(
            DataTransformationFunctions.validate_release_year
        )
        
        # Test content age calculation
        transformed_df['content_age'] = transformed_df['release_year_validated'].apply(
            DataTransformationFunctions.calculate_content_age
        )
        
        # Test duration extraction
        transformed_df['duration_minutes'] = transformed_df.apply(
            lambda row: DataTransformationFunctions.extract_duration_minutes(
                row['duration'], row['type']
            ), axis=1
        )
        
        # Validation tests
        # Test 1: No data loss during transformation
        test_results['no_data_loss'] = len(transformed_df) == len(self.df)
        
        # Test 2: Transformations improved data quality
        original_nulls = self.df['title'].isnull().sum()
        cleaned_nulls = transformed_df['title_cleaned'].isnull().sum()
        test_results['title_cleaning_effective'] = cleaned_nulls <= original_nulls
        
        # Test 3: Release year validation worked
        invalid_years_original = len(self.df[
            (self.df['release_year'] < 1900) | (self.df['release_year'] > datetime.now().year)
        ])
        invalid_years_cleaned = len(transformed_df[
            (transformed_df['release_year_validated'] < 1900) | 
            (transformed_df['release_year_validated'] > datetime.now().year)
        ])
        test_results['year_validation_effective'] = invalid_years_cleaned < invalid_years_original
        
        # Test 4: Content age calculation is reasonable
        valid_ages = transformed_df['content_age'].dropna()
        test_results['age_calculation_reasonable'] = (valid_ages >= 0).all() and (valid_ages <= 150).all()
        
        self.processed_df = transformed_df
        return test_results
    
    def test_data_quality_rules(self):
        """Test business logic and quality rules"""
        if self.processed_df is None:
            self.test_data_transformation()
        
        test_results = {}
        
        # Business Rule 1: Movies should have duration in minutes
        movies = self.processed_df[self.processed_df['type'] == 'Movie']
        movies_with_duration = movies['duration_minutes'].notna().sum()
        total_movies = len(movies)
        test_results['movies_duration_rule'] = {
            'pass_rate': (movies_with_duration / total_movies * 100) if total_movies > 0 else 0,
            'threshold': 80,  # 80% of movies should have valid duration
            'passed': (movies_with_duration / total_movies * 100) >= 80 if total_movies > 0 else False
        }
        
        # Business Rule 2: Content should have reasonable release years
        valid_years = self.processed_df['release_year_validated'].notna().sum()
        total_content = len(self.processed_df)
        test_results['release_year_rule'] = {
            'pass_rate': (valid_years / total_content * 100),
            'threshold': 95,  # 95% should have valid release years
            'passed': (valid_years / total_content * 100) >= 95
        }
        
        # Business Rule 3: All content should have titles
        valid_titles = self.processed_df['title_cleaned'].notna().sum()
        test_results['title_completeness_rule'] = {
            'pass_rate': (valid_titles / total_content * 100),
            'threshold': 100,  # 100% should have titles
            'passed': (valid_titles / total_content * 100) >= 100
        }
        
        # Business Rule 4: show_id should be unique
        unique_ids = self.processed_df['show_id'].nunique()
        test_results['unique_id_rule'] = {
            'pass_rate': (unique_ids / total_content * 100),
            'threshold': 100,  # 100% should be unique
            'passed': unique_ids == total_content
        }
        
        return test_results
    
    def run_integration_tests(self):
        """Run all integration tests"""
        print("🧪 Running Integration Tests")
        print("=" * 50)
        
        # Test ingestion
        print("1. Testing Data Ingestion...")
        ingestion_results = self.test_data_ingestion()
        
        # Test transformation
        print("2. Testing Data Transformation...")
        transformation_results = self.test_data_transformation()
        
        # Test quality rules
        print("3. Testing Quality Rules...")
        quality_results = self.test_data_quality_rules()
        
        # Compile results
        all_results = {
            'timestamp': datetime.now().isoformat(),
            'ingestion_tests': ingestion_results,
            'transformation_tests': transformation_results,
            'quality_rule_tests': quality_results
        }
        
        # Print summary
        self._print_test_summary(all_results)
        
        # Save results
        with open('reports/integration_test_results.json', 'w') as f:
            json.dump(all_results, f, indent=2)
        
        return all_results
    
    def _print_test_summary(self, results):
        """Print test summary"""
        print("\n📊 INTEGRATION TEST SUMMARY")
        print("=" * 50)
        
        # Ingestion tests
        ingestion = results['ingestion_tests']
        print(f"📥 Data Ingestion:")
        print(f"  ✅ File Readable: {ingestion.get('file_readable', False)}")
        print(f"  ✅ Schema Valid: {ingestion.get('schema_valid', False)}")
        print(f"  ✅ Sufficient Data: {ingestion.get('sufficient_data', False)}")
        
        # Transformation tests
        transformation = results['transformation_tests']
        print(f"\n🔄 Data Transformation:")
        print(f"  ✅ No Data Loss: {transformation.get('no_data_loss', False)}")
        print(f"  ✅ Title Cleaning: {transformation.get('title_cleaning_effective', False)}")
        print(f"  ✅ Year Validation: {transformation.get('year_validation_effective', False)}")
        print(f"  ✅ Age Calculation: {transformation.get('age_calculation_reasonable', False)}")
        
        # Quality rule tests
        quality = results['quality_rule_tests']
        print(f"\n📋 Quality Rules:")
        for rule_name, rule_data in quality.items():
            if isinstance(rule_data, dict) and 'passed' in rule_data:
                status = "✅" if rule_data['passed'] else "❌"
                print(f"  {status} {rule_name.replace('_', ' ').title()}: {rule_data['pass_rate']:.1f}% (threshold: {rule_data['threshold']}%)")

class EndToEndTester:
    """End-to-end testing for complete data pipeline"""
    
    def __init__(self, data_path='data/netflix_titles.csv'):
        self.data_path = data_path
        self.pipeline_results = {}
    
    def simulate_data_pipeline(self):
        """Simulate complete data pipeline execution"""
        print("🔄 Simulating Complete Data Pipeline...")
        
        pipeline_steps = []
        
        # Step 1: Data Ingestion
        try:
            df = pd.read_csv(self.data_path)
            pipeline_steps.append({
                'step': 'ingestion',
                'status': 'success',
                'rows_processed': len(df),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            pipeline_steps.append({
                'step': 'ingestion',
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return pipeline_steps
        
        # Step 2: Data Validation
        try:
            # Basic validation
            assert len(df) > 0, "Dataset is empty"
            assert 'show_id' in df.columns, "Missing show_id column"
            assert 'title' in df.columns, "Missing title column"
            
            pipeline_steps.append({
                'step': 'validation',
                'status': 'success',
                'checks_passed': 3,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            pipeline_steps.append({
                'step': 'validation',
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return pipeline_steps
        
        # Step 3: Data Transformation
        try:
            # Apply transformations
            df['title_cleaned'] = df['title'].apply(DataTransformationFunctions.clean_title)
            df['country_standardized'] = df['country'].apply(DataTransformationFunctions.standardize_country)
            df['release_year_validated'] = df['release_year'].apply(DataTransformationFunctions.validate_release_year)
            df['content_age'] = df['release_year_validated'].apply(DataTransformationFunctions.calculate_content_age)
            
            pipeline_steps.append({
                'step': 'transformation',
                'status': 'success',
                'transformations_applied': 4,
                'rows_processed': len(df),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            pipeline_steps.append({
                'step': 'transformation',
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return pipeline_steps
        
        # Step 4: Quality Assessment
        try:
            quality_score = self._calculate_pipeline_quality_score(df)
            
            pipeline_steps.append({
                'step': 'quality_assessment',
                'status': 'success',
                'quality_score': quality_score,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            pipeline_steps.append({
                'step': 'quality_assessment',
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return pipeline_steps
        
        # Step 5: Data Loading (Simulated)
        try:
            # Simulate saving to different targets
            output_file = 'data/processed_netflix_titles.csv'
            df.to_csv(output_file, index=False)
            
            pipeline_steps.append({
                'step': 'loading',
                'status': 'success',
                'output_file': output_file,
                'rows_loaded': len(df),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            pipeline_steps.append({
                'step': 'loading',
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        
        self.pipeline_results = {
            'pipeline_execution': pipeline_steps,
            'overall_status': 'success' if all(step['status'] == 'success' for step in pipeline_steps) else 'failed',
            'execution_time': datetime.now().isoformat()
        }
        
        return pipeline_steps
    
    def _calculate_pipeline_quality_score(self, df):
        """Calculate quality score for pipeline output"""
        scores = []
        
        # Completeness score
        critical_fields = ['show_id', 'title_cleaned', 'type']
        completeness = sum(df[field].notna().sum() / len(df) for field in critical_fields) / len(critical_fields)
        scores.append(completeness * 100)
        
        # Validity score
        valid_years = df['release_year_validated'].notna().sum() / len(df)
        valid_ids = df['show_id'].str.match(r'^s\d+, na=False).sum() / len(df)
        validity = (valid_years + valid_ids) / 2
        scores.append(validity * 100)
        
        # Uniqueness score
        unique_ids = df['show_id'].nunique() / len(df)
        scores.append(unique_ids * 100)
        
        return np.mean(scores)
    
    def test_pipeline_performance(self):
        """Test pipeline performance characteristics"""
        print("⚡ Testing Pipeline Performance...")
        
        # Measure processing time
        start_time = datetime.now()
        pipeline_steps = self.simulate_data_pipeline()
        end_time = datetime.now()
        
        processing_time = (end_time - start_time).total_seconds()
        
        # Load data for volume testing
        df = pd.read_csv(self.data_path)
        
        performance_metrics = {
            'processing_time_seconds': processing_time,
            'rows_per_second': len(df) / processing_time if processing_time > 0 else 0,
            'data_volume_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'throughput_mb_per_second': (df.memory_usage(deep=True).sum() / 1024 / 1024) / processing_time if processing_time > 0 else 0
        }
        
        # Performance benchmarks
        benchmarks = {
            'max_processing_time_seconds': 60,  # Should process within 1 minute
            'min_rows_per_second': 100,  # Should process at least 100 rows per second
            'max_memory_usage_mb': 500   # Should use less than 500MB
        }
        
        performance_results = {
            'metrics': performance_metrics,
            'benchmarks': benchmarks,
            'performance_passed': (
                performance_metrics['processing_time_seconds'] <= benchmarks['max_processing_time_seconds'] and
                performance_metrics['rows_per_second'] >= benchmarks['min_rows_per_second'] and
                performance_metrics['data_volume_mb'] <= benchmarks['max_memory_usage_mb']
            )
        }
        
        return performance_results
    
    def test_error_handling(self):
        """Test pipeline error handling and recovery"""
        print("🛡️ Testing Error Handling...")
        
        error_tests = []
        
        # Test 1: Missing file
        try:
            pd.read_csv('nonexistent_file.csv')
            error_tests.append({'test': 'missing_file', 'handled': False})
        except FileNotFoundError:
            error_tests.append({'test': 'missing_file', 'handled': True})
        except Exception as e:
            error_tests.append({'test': 'missing_file', 'handled': False, 'unexpected_error': str(e)})
        
        # Test 2: Corrupted data (empty DataFrame)
        try:
            empty_df = pd.DataFrame()
            assert len(empty_df) > 0, "Empty dataset"
            error_tests.append({'test': 'empty_data', 'handled': False})
        except AssertionError:
            error_tests.append({'test': 'empty_data', 'handled': True})
        except Exception as e:
            error_tests.append({'test': 'empty_data', 'handled': False, 'unexpected_error': str(e)})
        
        # Test 3: Invalid data types
        try:
            test_df = pd.DataFrame({'release_year': ['invalid', 'data', 'types']})
            validated_years = test_df['release_year'].apply(DataTransformationFunctions.validate_release_year)
            # Should handle gracefully by returning None for invalid values
            error_tests.append({'test': 'invalid_data_types', 'handled': validated_years.isna().all()})
        except Exception as e:
            error_tests.append({'test': 'invalid_data_types', 'handled': False, 'error': str(e)})
        
        error_handling_results = {
            'tests': error_tests,
            'error_handling_score': sum(1 for test in error_tests if test['handled']) / len(error_tests) * 100
        }
        
        return error_handling_results
    
    def run_end_to_end_tests(self):
        """Run complete end-to-end test suite"""
        print("🎯 Running End-to-End Tests")
        print("=" * 50)
        
        # Run pipeline simulation
        print("1. Running Pipeline Simulation...")
        pipeline_steps = self.simulate_data_pipeline()
        
        # Test performance
        print("2. Testing Performance...")
        performance_results = self.test_pipeline_performance()
        
        # Test error handling
        print("3. Testing Error Handling...")
        error_handling_results = self.test_error_handling()
        
        # Compile all results
        e2e_results = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_execution': self.pipeline_results,
            'performance_tests': performance_results,
            'error_handling_tests': error_handling_results,
            'overall_e2e_status': self._determine_overall_status(pipeline_steps, performance_results, error_handling_results)
        }
        
        # Print summary
        self._print_e2e_summary(e2e_results)
        
        # Save results
        with open('reports/e2e_test_results.json', 'w') as f:
            json.dump(e2e_results, f, indent=2, default=str)
        
        return e2e_results
    
    def _determine_overall_status(self, pipeline_steps, performance_results, error_handling_results):
        """Determine overall E2E test status"""
        pipeline_success = all(step['status'] == 'success' for step in pipeline_steps)
        performance_success = performance_results['performance_passed']
        error_handling_success = error_handling_results['error_handling_score'] >= 80  # 80% threshold
        
        if pipeline_success and performance_success and error_handling_success:
            return 'PASSED'
        elif pipeline_success:
            return 'PASSED_WITH_WARNINGS'
        else:
            return 'FAILED'
    
    def _print_e2e_summary(self, results):
        """Print end-to-end test summary"""
        print("\n🎯 END-TO-END TEST SUMMARY")
        print("=" * 50)
        
        # Pipeline execution
        pipeline = results['pipeline_execution']
        print(f"🔄 Pipeline Execution: {pipeline['overall_status'].upper()}")
        for step in pipeline['pipeline_execution']:
            status_icon = "✅" if step['status'] == 'success' else "❌"
            print(f"  {status_icon} {step['step'].replace('_', ' ').title()}")
        
        # Performance
        performance = results['performance_tests']
        perf_icon = "✅" if performance['performance_passed'] else "❌"
        print(f"\n⚡ Performance Tests: {perf_icon}")
        print(f"  Processing Time: {performance['metrics']['processing_time_seconds']:.2f}s")
        print(f"  Throughput: {performance['metrics']['rows_per_second']:.0f} rows/sec")
        
        # Error handling
        error_handling = results['error_handling_tests']
        error_score = error_handling['error_handling_score']
        error_icon = "✅" if error_score >= 80 else "❌"
        print(f"\n🛡️ Error Handling: {error_icon} ({error_score:.0f}%)")
        
        print(f"\n🏆 OVERALL STATUS: {results['overall_e2e_status']}")

def run_complete_test_suite():
    """Run the complete testing suite"""
    print("🧪 NETFLIX DATA QUALITY TESTING SUITE")
    print("=" * 60)
    
    # Create reports directory if it doesn't exist
    import os
    os.makedirs('reports', exist_ok=True)
    
    # 1. Run Unit Tests
    print("\n1️⃣ RUNNING UNIT TESTS")
    print("-" * 30)
    
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestDataTransformationFunctions)
    test_runner = unittest.TextTestRunner(verbosity=2)
    unit_test_results = test_runner.run(test_suite)
    
    # 2. Run Integration Tests
    print("\n2️⃣ RUNNING INTEGRATION TESTS")
    print("-" * 30)
    
    integration_tester = DataPipelineIntegrationTester()
    integration_results = integration_tester.run_integration_tests()
    
    # 3. Run End-to-End Tests
    print("\n3️⃣ RUNNING END-TO-END TESTS")
    print("-" * 30)
    
    e2e_tester = EndToEndTester()
    e2e_results = e2e_tester.run_end_to_end_tests()
    
    # Generate final summary
    print("\n" + "="*60)
    print("📊 COMPLETE TEST SUITE SUMMARY")
    print("="*60)
    
    unit_success = unit_test_results.wasSuccessful()
    integration_success = all(
        integration_results['ingestion_tests'].get('file_readable', False),
        integration_results['transformation_tests'].get('no_data_loss', False)
    )
    e2e_success = e2e_results['overall_e2e_status'] in ['PASSED', 'PASSED_WITH_WARNINGS']
    
    print(f"🔬 Unit Tests: {'✅ PASSED' if unit_success else '❌ FAILED'}")
    print(f"🔗 Integration Tests: {'✅ PASSED' if integration_success else '❌ FAILED'}")
    print(f"🎯 End-to-End Tests: {'✅ PASSED' if e2e_success else '❌ FAILED'}")
    
    overall_success = unit_success and integration_success and e2e_success
    print(f"\n🏆 OVERALL TESTING STATUS: {'✅ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}")
    
    print("\n📋 Test reports saved to 'reports/' directory")
    print("="*60)
    
    return {
        'unit_tests': unit_success,
        'integration_tests': integration_success,
        'end_to_end_tests': e2e_success,
        'overall_success': overall_success
    }

if __name__ == "__main__":
    # Run the complete test suite
    test_results = run_complete_test_suite()
