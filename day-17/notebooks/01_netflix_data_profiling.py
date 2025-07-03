# Netflix Data Profiling and Quality Assessment
# Save this as: notebooks/01_netflix_data_profiling.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import great_expectations as gx

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

def load_and_explore_data():
    """Load Netflix data and perform initial exploration"""
    print("🎬 Loading Netflix Dataset...")
    
    # Load the data
    df = pd.read_csv('data/netflix_titles.csv')
    
    print(f"Dataset Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    return df

def data_quality_assessment(df):
    """Perform comprehensive data quality assessment"""
    
    print("\n📊 DATA QUALITY ASSESSMENT")
    print("=" * 50)
    
    # 1. COMPLETENESS Analysis
    print("\n1. COMPLETENESS ANALYSIS")
    print("-" * 30)
    
    missing_data = df.isnull().sum()
    missing_percentage = (missing_data / len(df)) * 100
    
    completeness_report = pd.DataFrame({
        'Column': df.columns,
        'Missing_Count': missing_data.values,
        'Missing_Percentage': missing_percentage.values,
        'Data_Type': df.dtypes.values
    }).sort_values('Missing_Percentage', ascending=False)
    
    print(completeness_report)
    
    # 2. VALIDITY Analysis
    print("\n2. VALIDITY ANALYSIS")
    print("-" * 30)
    
    validity_issues = {}
    
    # Check release_year validity
    current_year = datetime.now().year
    invalid_years = df[(df['release_year'] < 1900) | (df['release_year'] > current_year)]
    validity_issues['invalid_release_years'] = len(invalid_years)
    
    # Check type validity
    valid_types = ['Movie', 'TV Show']
    invalid_types = df[~df['type'].isin(valid_types)]
    validity_issues['invalid_types'] = len(invalid_types)
    
    # Check duration format
    movie_duration_issues = df[(df['type'] == 'Movie') & (~df['duration'].str.contains('min', na=True))]
    tv_duration_issues = df[(df['type'] == 'TV Show') & (~df['duration'].str.contains('Season', na=True))]
    validity_issues['duration_format_issues'] = len(movie_duration_issues) + len(tv_duration_issues)
    
    for issue, count in validity_issues.items():
        print(f"{issue}: {count} records")
    
    # 3. UNIQUENESS Analysis
    print("\n3. UNIQUENESS ANALYSIS")
    print("-" * 30)
    
    # Check for duplicate show_ids
    duplicate_ids = df[df['show_id'].duplicated()]
    print(f"Duplicate show_ids: {len(duplicate_ids)}")
    
    # Check for potential duplicate content
    duplicate_content = df[df.duplicated(subset=['title', 'release_year', 'type'], keep=False)]
    print(f"Potential duplicate content: {len(duplicate_content)}")
    
    # 4. CONSISTENCY Analysis
    print("\n4. CONSISTENCY ANALYSIS")
    print("-" * 30)
    
    # Country format consistency
    country_formats = df['country'].dropna().apply(lambda x: 'Multiple' if ',' in str(x) else 'Single')
    print(f"Country format distribution:\n{country_formats.value_counts()}")
    
    # Date format consistency
    date_patterns = df['date_added'].dropna().apply(lambda x: len(str(x).split()))
    print(f"Date format patterns:\n{date_patterns.value_counts()}")
    
    return completeness_report, validity_issues

def visualize_quality_issues(df):
    """Create visualizations for data quality issues"""
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Netflix Data Quality Assessment', fontsize=16, fontweight='bold')
    
    # 1. Missing Data Heatmap
    missing_data = df.isnull()
    sns.heatmap(missing_data, cbar=True, ax=axes[0,0], cmap='viridis')
    axes[0,0].set_title('Missing Data Pattern')
    axes[0,0].set_xlabel('Columns')
    
    # 2. Completeness by Column
    completeness = (1 - df.isnull().sum() / len(df)) * 100
    completeness.plot(kind='bar', ax=axes[0,1], color='skyblue')
    axes[0,1].set_title('Data Completeness by Column')
    axes[0,1].set_ylabel('Completeness %')
    axes[0,1].tick_params(axis='x', rotation=45)
    
    # 3. Content Type Distribution
    df['type'].value_counts().plot(kind='pie', ax=axes[1,0], autopct='%1.1f%%')
    axes[1,0].set_title('Content Type Distribution')
    
    # 4. Release Year Distribution
    df['release_year'].hist(bins=30, ax=axes[1,1], color='lightcoral', edgecolor='black')
    axes[1,1].set_title('Release Year Distribution')
    axes[1,1].set_xlabel('Release Year')
    axes[1,1].set_ylabel('Frequency')
    
    plt.tight_layout()
    plt.savefig('reports/netflix_quality_assessment.png', dpi=300, bbox_inches='tight')
    plt.show()

def generate_quality_report(df, completeness_report, validity_issues):
    """Generate a comprehensive quality report"""
    
    report = f"""
# Netflix Dataset Quality Report
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Dataset Overview
- **Total Records**: {len(df):,}
- **Total Columns**: {len(df.columns)}
- **Date Range**: {df['release_year'].min()} - {df['release_year'].max()}

## Quality Dimensions Assessment

### 1. Completeness Score: {((df.notna().sum().sum()) / (df.shape[0] * df.shape[1]) * 100):.2f}%

**Fields with Missing Data:**
{completeness_report[completeness_report['Missing_Percentage'] > 0].to_string()}

### 2. Validity Issues Found:
"""
    
    for issue, count in validity_issues.items():
        report += f"- **{issue.replace('_', ' ').title()}**: {count} records\n"
    
    report += f"""
### 3. Uniqueness Assessment:
- **Unique show_ids**: {df['show_id'].nunique():,} out of {len(df):,}
- **Unique titles**: {df['title'].nunique():,}

### 4. Business Impact Assessment:
- **Critical Issues**: {sum(1 for v in validity_issues.values() if v > 0)}
- **Data Usability**: {'Good' if sum(validity_issues.values()) < 100 else 'Needs Attention'}

## Recommendations:
1. Address missing director and cast information for better recommendations
2. Standardize country name formats
3. Validate and correct invalid release years
4. Implement data validation rules for duration formats
"""
    
    # Save report
    with open('reports/netflix_quality_report.md', 'w') as f:
        f.write(report)
    
    print("📋 Quality report saved to: reports/netflix_quality_report.md")
    return report

def main():
    """Main execution function"""
    print("🛡️ Day 17: Netflix Data Quality Assessment")
    print("=" * 60)
    
    # Load data
    df = load_and_explore_data()
    
    # Perform quality assessment
    completeness_report, validity_issues = data_quality_assessment(df)
    
    # Create visualizations
    visualize_quality_issues(df)
    
    # Generate report
    report = generate_quality_report(df, completeness_report, validity_issues)
    
    print("\n✅ Data quality assessment completed!")
    print("📊 Check the 'reports/' folder for detailed analysis")
    
    return df, completeness_report, validity_issues

if __name__ == "__main__":
    # Run the assessment
    df, completeness_report, validity_issues = main()
