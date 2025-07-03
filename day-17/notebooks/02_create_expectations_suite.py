# Netflix Great Expectations Suite Implementation
# Save this as: notebooks/02_create_expectations_suite.py

import great_expectations as gx
import pandas as pd
from datetime import datetime

def create_netflix_expectations_suite():
    """Create a comprehensive expectations suite for Netflix data"""
    
    print("🎯 Creating Netflix Data Quality Expectations Suite")
    print("=" * 60)
    
    # Initialize Great Expectations context
    context = gx.get_context()
    
    # Connect to our datasource
    datasource = context.get_datasource("netflix_datasource")
    
    # Get data asset
    data_asset = datasource.get_asset("netflix_titles")
    
    # Create batch request
    batch_request = data_asset.build_batch_request()
    
    # Create expectation suite
    suite_name = "netflix_quality_suite"
    suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    
    # Create validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Adding Expectations...")
    
    # 1. STRUCTURAL EXPECTATIONS
    print("1. Adding structural expectations...")
    
    # Table should exist and have data
    validator.expect_table_row_count_to_be_between(min_value=1000, max_value=50000)
    
    # Expected columns should exist
    expected_columns = [
        "show_id", "type", "title", "director", "cast", 
        "country", "date_added", "release_year", "rating", 
        "duration", "listed_in", "description"
    ]
    validator.expect_table_columns_to_match_ordered_list(column_list=expected_columns)
    
    # 2. UNIQUENESS EXPECTATIONS
    print("2. Adding uniqueness expectations...")
    
    # show_id should be unique
    validator.expect_column_values_to_be_unique(column="show_id")
    
    # 3. COMPLETENESS EXPECTATIONS (Business-driven thresholds)
    print("3. Adding completeness expectations...")
    
    # Critical fields (100% required)
    validator.expect_column_values_to_not_be_null(column="show_id")
    validator.expect_column_values_to_not_be_null(column="title")
    validator.expect_column_values_to_not_be_null(column="type")
    
    # Important fields (high expectation but some tolerance)
    validator.expect_column_values_to_not_be_null(column="director", mostly=0.7)  # 70% expected
    validator.expect_column_values_to_not_be_null(column="cast", mostly=0.8)      # 80% expected
    validator.expect_column_values_to_not_be_null(column="release_year", mostly=0.99)  # 99% expected
    
    # 4. VALIDITY EXPECTATIONS
    print("4. Adding validity expectations...")
    
    # Type should only be Movie or TV Show
    validator.expect_column_values_to_be_in_set(
        column="type", 
        value_set=["Movie", "TV Show"]
    )
    
    # Release year should be reasonable
    current_year = datetime.now().year
    validator.expect_column_values_to_be_between(
        column="release_year",
        min_value=1900,
        max_value=current_year
    )
    
    # Rating should be from valid set
    valid_ratings = [
        "G", "PG", "PG-13", "R", "NC-17",  # Movie ratings
        "TV-Y", "TV-Y7", "TV-G", "TV-PG", "TV-14", "TV-MA",  # TV ratings
        "NR", "UR", "NOT RATED"  # Unrated content
    ]
    validator.expect_column_values_to_be_in_set(
        column="rating",
        value_set=valid_ratings,
        mostly=0.95  # Allow some flexibility for international content
    )
    
    # show_id should follow expected pattern (s1, s2, etc.)
    validator.expect_column_values_to_match_regex(
        column="show_id",
        regex=r"^s\d+$"
    )
    
    # 5. BUSINESS LOGIC EXPECTATIONS
    print("5. Adding business logic expectations...")
    
    # Title should not be empty string
    validator.expect_column_values_to_not_match_regex(
        column="title",
        regex=r"^\s*$"  # Empty or whitespace only
    )
    
    # Duration format validation would require custom expectations
    # For now, we'll check that duration exists for most records
    validator.expect_column_values_to_not_be_null(column="duration", mostly=0.95)
    
    # 6. STATISTICAL EXPECTATIONS
    print("6. Adding statistical expectations...")
    
    # Most content should be relatively recent
    validator.expect_column_mean_to_be_between(
        column="release_year",
        min_value=1990,
        max_value=current_year
    )
    
    # Title length should be reasonable
    validator.expect_column_value_lengths_to_be_between(
        column="title",
        min_value=1,
        max_value=200
    )
    
    print("✅ Expectations suite created successfully!")
    
    # Save the suite
    context.save_expectation_suite(expectation_suite=suite)
    
    return validator, suite

def run_validation(validator):
    """Run the validation and generate results"""
    
    print("\n🔍 Running Data Validation...")
    print("=" * 40)
    
    # Run validation
    results = validator.validate()
    
    # Print summary
    print(f"Validation Results:")
    print(f"✅ Successful expectations: {results.statistics['successful_expectations']}")
    print(f"❌ Failed expectations: {results.statistics['unsuccessful_expectations']}")
    print(f"📊 Success rate: {results.statistics['success_percent']:.2f}%")
    
    # Print details of failed expectations
    if results.statistics['unsuccessful_expectations'] > 0:
        print("\n❌ Failed Expectations Details:")
        for result in results.results:
            if not result.success:
                print(f"- {result.expectation_config.expectation_type}")
                print(f"  Column: {result.expectation_config.kwargs.get('column', 'N/A')}")
                print(f"  Details: {result.result}")
                print()
    
    return results

def create_checkpoint():
    """Create a checkpoint for automated validation"""
    
    print("\n🚨 Creating Validation Checkpoint...")
    
    context = gx.get_context()
    
    # Create checkpoint configuration
    checkpoint_config = {
        "name": "netflix_quality_checkpoint",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-netflix-quality-check",
        "expectation_suite_name": "netflix_quality_suite",
        "batch_request": {},
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "netflix_datasource",
                    "data_asset_name": "netflix_titles",
                },
            }
        ],
    }
    
    # Add checkpoint
    checkpoint = context.add_checkpoint(**checkpoint_config)
    
    print("✅ Checkpoint created successfully!")
    return checkpoint

def main():
    """Main execution function"""
    
    try:
        # Create expectations suite
        validator, suite = create_netflix_expectations_suite()
        
        # Run validation
        results = run_validation(validator)
        
        # Create checkpoint for automation
        checkpoint = create_checkpoint()
        
        # Generate data docs
        print("\n📚 Generating Data Documentation...")
        context = gx.get_context()
        context.build_data_docs()
        
        print("\n🎉 Great Expectations setup completed successfully!")
        print("📖 Open great_expectations/uncommitted/data_docs/local_site/index.html to view results")
        
        return validator, results, checkpoint
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        print("💡 Make sure Great Expectations is properly initialized")
        return None, None, None

if __name__ == "__main__":
    validator, results, checkpoint = main()
