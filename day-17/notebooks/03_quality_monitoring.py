# Data Quality Monitoring System
# Save this as: notebooks/03_quality_monitoring.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

class DataQualityMonitor:
    """Data Quality Monitoring System for Netflix Dataset"""
    
    def __init__(self, data_path='data/netflix_titles.csv'):
        self.data_path = data_path
        self.df = None
        self.quality_metrics = {}
        self.thresholds = {
            'completeness': 0.8,      # 80% completeness threshold
            'validity': 0.95,         # 95% validity threshold
            'uniqueness': 0.99,       # 99% uniqueness threshold
            'consistency': 0.9        # 90% consistency threshold
        }
    
    def load_data(self):
        """Load the Netflix dataset"""
        self.df = pd.read_csv(self.data_path)
        print(f"📊 Loaded dataset: {self.df.shape[0]} rows, {self.df.shape[1]} columns")
    
    def calculate_completeness_score(self):
        """Calculate completeness score for each column"""
        if self.df is None:
            self.load_data()
        
        completeness = {}
        for col in self.df.columns:
            non_null_count = self.df[col].notna().sum()
            total_count = len(self.df)
            completeness[col] = (non_null_count / total_count) * 100
        
        # Calculate weighted completeness score
        # Critical fields have higher weight
        critical_fields = ['show_id', 'title', 'type']
        important_fields = ['director', 'cast', 'release_year', 'rating']
        optional_fields = ['country', 'date_added', 'duration', 'listed_in', 'description']
        
        weighted_score = 0
        total_weight = 0
        
        for field in critical_fields:
            if field in completeness:
                weighted_score += completeness[field] * 3  # Weight of 3
                total_weight += 3
        
        for field in important_fields:
            if field in completeness:
                weighted_score += completeness[field] * 2  # Weight of 2
                total_weight += 2
        
        for field in optional_fields:
            if field in completeness:
                weighted_score += completeness[field] * 1  # Weight of 1
                total_weight += 1
        
        overall_completeness = weighted_score / total_weight if total_weight > 0 else 0
        
        self.quality_metrics['completeness'] = {
            'score': overall_completeness,
            'by_column': completeness,
            'status': 'PASS' if overall_completeness >= self.thresholds['completeness'] * 100 else 'FAIL'
        }
        
        return completeness, overall_completeness
    
    def calculate_validity_score(self):
        """Calculate validity score based on business rules"""
        if self.df is None:
            self.load_data()
        
        validity_checks = {}
        
        # 1. Type validation
        valid_types = ['Movie', 'TV Show']
        type_validity = self.df['type'].isin(valid_types).sum() / len(self.df) * 100
        validity_checks['type_format'] = type_validity
        
        # 2. Release year validation
        current_year = datetime.now().year
        year_validity = self.df[
            (self.df['release_year'] >= 1900) & 
            (self.df['release_year'] <= current_year)
        ].shape[0] / len(self.df) * 100
        validity_checks['release_year_range'] = year_validity
        
        # 3. Show ID format validation
        id_validity = self.df['show_id'].str.match(r'^s\d+$', na=False).sum() / len(self.df) * 100
        validity_checks['show_id_format'] = id_validity
        
        # 4. Title validation (not empty)
        title_validity = (
            self.df['title'].notna() & 
            (self.df['title'].str.strip() != '')
        ).sum() / len(self.df) * 100
        validity_checks['title_not_empty'] = title_validity
        
        # Calculate overall validity score
        overall_validity = np.mean(list(validity_checks.values()))
        
        self.quality_metrics['validity'] = {
            'score': overall_validity,
            'by_check': validity_checks,
            'status': 'PASS' if overall_validity >= self.thresholds['validity'] * 100 else 'FAIL'
        }
        
        return validity_checks, overall_validity
    
    def calculate_uniqueness_score(self):
        """Calculate uniqueness score"""
        if self.df is None:
            self.load_data()
        
        uniqueness_checks = {}
        
        # 1. show_id uniqueness
        unique_ids = self.df['show_id'].nunique()
        total_ids = len(self.df)
        id_uniqueness = (unique_ids / total_ids) * 100
        uniqueness_checks['show_id_uniqueness'] = id_uniqueness
        
        # 2. Title uniqueness (considering same title might have different years)
        unique_titles = self.df['title'].nunique()
        total_titles = len(self.df)
        title_uniqueness = (unique_titles / total_titles) * 100
        uniqueness_checks['title_uniqueness'] = title_uniqueness
        
        # 3. Combination uniqueness (title + year + type)
        unique_combinations = self.df[['title', 'release_year', 'type']].drop_duplicates().shape[0]
        combination_uniqueness = (unique_combinations / len(self.df)) * 100
        uniqueness_checks['content_combination_uniqueness'] = combination_uniqueness
        
        # Overall uniqueness score (weighted by importance)
        overall_uniqueness = (
            id_uniqueness * 0.5 +  # show_id most important
            combination_uniqueness * 0.3 +  # content combination important
            title_uniqueness * 0.2  # title uniqueness less critical
        )
        
        self.quality_metrics['uniqueness'] = {
            'score': overall_uniqueness,
            'by_check': uniqueness_checks,
            'status': 'PASS' if overall_uniqueness >= self.thresholds['uniqueness'] * 100 else 'FAIL'
        }
        
        return uniqueness_checks, overall_uniqueness
    
    def calculate_consistency_score(self):
        """Calculate consistency score"""
        if self.df is None:
            self.load_data()
        
        consistency_checks = {}
        
        # 1. Date format consistency
        date_pattern_consistent = 0
        if 'date_added' in self.df.columns:
            # Check if dates follow "Month Day, Year" pattern
            date_mask = self.df['date_added'].notna()
            if date_mask.sum() > 0:
                date_pattern_matches = self.df.loc[date_mask, 'date_added'].str.match(
                    r'^[A-Za-z]+ \d{1,2}, \d{4}$'
                ).sum()
                date_pattern_consistent = (date_pattern_matches / date_mask.sum()) * 100
        consistency_checks['date_format_consistency'] = date_pattern_consistent
        
        # 2. Country format consistency (check for standardization)
        country_consistency = 100  # Default to 100% if no issues found
        if 'country' in self.df.columns:
            country_mask = self.df['country'].notna()
            if country_mask.sum() > 0:
                # Check for consistent country naming (basic check)
                countries = self.df.loc[country_mask, 'country']
                # Count entries that don't have inconsistent formatting
                clean_countries = countries.str.strip().str.title()
                country_consistency = (clean_countries == countries).sum() / len(countries) * 100
        consistency_checks['country_format_consistency'] = country_consistency
        
        # 3. Duration format consistency
        duration_consistency = 0
        if 'duration' in self.df.columns and 'type' in self.df.columns:
            # Movies should have "X min" format
            movie_mask = (self.df['type'] == 'Movie') & self.df['duration'].notna()
            if movie_mask.sum() > 0:
                movie_duration_correct = self.df.loc[movie_mask, 'duration'].str.contains(
                    r'\d+ min, na=False
                ).sum()
                movie_consistency = (movie_duration_correct / movie_mask.sum()) * 100
            else:
                movie_consistency = 100
            
            # TV Shows should have "X Season(s)" format
            tv_mask = (self.df['type'] == 'TV Show') & self.df['duration'].notna()
            if tv_mask.sum() > 0:
                tv_duration_correct = self.df.loc[tv_mask, 'duration'].str.contains(
                    r'\d+ Season[s]?, na=False
                ).sum()
                tv_consistency = (tv_duration_correct / tv_mask.sum()) * 100
            else:
                tv_consistency = 100
            
            duration_consistency = (movie_consistency + tv_consistency) / 2
        consistency_checks['duration_format_consistency'] = duration_consistency
        
        # Overall consistency score
        overall_consistency = np.mean(list(consistency_checks.values()))
        
        self.quality_metrics['consistency'] = {
            'score': overall_consistency,
            'by_check': consistency_checks,
            'status': 'PASS' if overall_consistency >= self.thresholds['consistency'] * 100 else 'FAIL'
        }
        
        return consistency_checks, overall_consistency
    
    def calculate_overall_quality_score(self):
        """Calculate overall quality score"""
        # Ensure all metrics are calculated
        if 'completeness' not in self.quality_metrics:
            self.calculate_completeness_score()
        if 'validity' not in self.quality_metrics:
            self.calculate_validity_score()
        if 'uniqueness' not in self.quality_metrics:
            self.calculate_uniqueness_score()
        if 'consistency' not in self.quality_metrics:
            self.calculate_consistency_score()
        
        # Weight the different dimensions
        weights = {
            'completeness': 0.3,  # 30% weight
            'validity': 0.3,      # 30% weight
            'uniqueness': 0.25,   # 25% weight
            'consistency': 0.15   # 15% weight
        }
        
        overall_score = sum(
            self.quality_metrics[dimension]['score'] * weight
            for dimension, weight in weights.items()
        )
        
        # Determine overall status
        if overall_score >= 90:
            status = 'EXCELLENT'
        elif overall_score >= 80:
            status = 'GOOD'
        elif overall_score >= 70:
            status = 'ACCEPTABLE'
        else:
            status = 'NEEDS_IMPROVEMENT'
        
        self.quality_metrics['overall'] = {
            'score': overall_score,
            'status': status,
            'weights_used': weights
        }
        
        return overall_score, status
    
    def generate_quality_dashboard(self):
        """Generate quality monitoring dashboard"""
        if self.df is None:
            self.load_data()
        
        # Calculate all metrics
        self.calculate_overall_quality_score()
        
        # Create dashboard
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Netflix Data Quality Monitoring Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Overall Quality Score (Gauge chart)
        ax1 = axes[0, 0]
        overall_score = self.quality_metrics['overall']['score']
        colors = ['red' if overall_score < 70 else 'orange' if overall_score < 80 else 'green']
        
        # Create a simple gauge chart
        wedges = [overall_score, 100 - overall_score]
        colors_gauge = ['green' if overall_score >= 80 else 'orange' if overall_score >= 70 else 'red', 'lightgray']
        ax1.pie(wedges, colors=colors_gauge, startangle=90, counterclock=False)
        ax1.add_patch(plt.Circle((0, 0), 0.5, color='white'))
        ax1.text(0, 0, f'{overall_score:.1f}%', ha='center', va='center', fontsize=14, fontweight='bold')
        ax1.set_title('Overall Quality Score')
        
        # 2. Quality Dimensions Comparison
        ax2 = axes[0, 1]
        dimensions = ['Completeness', 'Validity', 'Uniqueness', 'Consistency']
        scores = [
            self.quality_metrics['completeness']['score'],
            self.quality_metrics['validity']['score'],
            self.quality_metrics['uniqueness']['score'],
            self.quality_metrics['consistency']['score']
        ]
        
        bars = ax2.bar(dimensions, scores, color=['skyblue', 'lightgreen', 'lightcoral', 'lightyellow'])
        ax2.set_ylim(0, 100)
        ax2.set_ylabel('Quality Score (%)')
        ax2.set_title('Quality Dimensions Breakdown')
        
        # Add threshold line
        ax2.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='Threshold')
        ax2.legend()
        
        # Add value labels on bars
        for bar, score in zip(bars, scores):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                    f'{score:.1f}%', ha='center', va='bottom')
        
        # 3. Completeness by Column
        ax3 = axes[0, 2]
        completeness_data = self.quality_metrics['completeness']['by_column']
        columns = list(completeness_data.keys())[:8]  # Show top 8 columns
        values = [completeness_data[col] for col in columns]
        
        ax3.barh(columns, values, color='lightblue')
        ax3.set_xlim(0, 100)
        ax3.set_xlabel('Completeness (%)')
        ax3.set_title('Data Completeness by Column')
        
        # 4. Validity Checks Status
        ax4 = axes[1, 0]
        validity_data = self.quality_metrics['validity']['by_check']
        checks = list(validity_data.keys())
        check_scores = list(validity_data.values())
        
        colors = ['green' if score >= 95 else 'orange' if score >= 85 else 'red' for score in check_scores]
        ax4.bar(range(len(checks)), check_scores, color=colors)
        ax4.set_xticks(range(len(checks)))
        ax4.set_xticklabels([check.replace('_', '\n') for check in checks], rotation=45, ha='right')
        ax4.set_ylim(0, 100)
        ax4.set_ylabel('Validity Score (%)')
        ax4.set_title('Validity Checks Status')
        
        # 5. Quality Trend (Simulated)
        ax5 = axes[1, 1]
        # Simulate quality trend over time
        dates = pd.date_range(start='2024-01-01', end='2024-07-01', freq='W')
        np.random.seed(42)
        trend_scores = overall_score + np.random.normal(0, 2, len(dates))
        trend_scores = np.clip(trend_scores, 0, 100)
        
        ax5.plot(dates, trend_scores, marker='o', linewidth=2, markersize=4)
        ax5.set_ylim(0, 100)
        ax5.set_ylabel('Quality Score (%)')
        ax5.set_title('Quality Score Trend (Simulated)')
        ax5.tick_params(axis='x', rotation=45)
        
        # Add threshold line
        ax5.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='Target')
        ax5.legend()
        
        # 6. Quality Status Summary
        ax6 = axes[1, 2]
        ax6.axis('off')
        
        # Create status summary
        status_text = f"""
Quality Status Summary

Overall Score: {overall_score:.1f}%
Status: {self.quality_metrics['overall']['status']}

Dimension Scores:
• Completeness: {self.quality_metrics['completeness']['score']:.1f}% ({self.quality_metrics['completeness']['status']})
• Validity: {self.quality_metrics['validity']['score']:.1f}% ({self.quality_metrics['validity']['status']})
• Uniqueness: {self.quality_metrics['uniqueness']['score']:.1f}% ({self.quality_metrics['uniqueness']['status']})
• Consistency: {self.quality_metrics['consistency']['score']:.1f}% ({self.quality_metrics['consistency']['status']})

Recommendations:
• Monitor completeness trends
• Validate data at source
• Implement automated checks
• Review quality thresholds
"""
        
        ax6.text(0.05, 0.95, status_text, transform=ax6.transAxes, fontsize=10,
                verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
        
        plt.tight_layout()
        plt.savefig('monitoring/dashboards/quality_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def save_quality_metrics(self):
        """Save quality metrics to JSON file"""
        timestamp = datetime.now().isoformat()
        
        # Prepare metrics for JSON serialization
        metrics_to_save = {
            'timestamp': timestamp,
            'dataset_info': {
                'rows': len(self.df),
                'columns': len(self.df.columns)
            },
            'quality_metrics': self.quality_metrics,
            'thresholds': self.thresholds
        }
        
        # Save to file
        with open('monitoring/quality_metrics_history.json', 'w') as f:
            json.dump(metrics_to_save, f, indent=2)
        
        print(f"📊 Quality metrics saved to monitoring/quality_metrics_history.json")
    
    def generate_quality_alerts(self):
        """Generate quality alerts based on thresholds"""
        alerts = []
        
        for dimension, metrics in self.quality_metrics.items():
            if dimension == 'overall':
                continue
                
            if metrics['status'] == 'FAIL':
                alert = {
                    'timestamp': datetime.now().isoformat(),
                    'level': 'HIGH' if metrics['score'] < 50 else 'MEDIUM',
                    'dimension': dimension,
                    'score': metrics['score'],
                    'threshold': self.thresholds[dimension] * 100,
                    'message': f"{dimension.title()} quality below threshold: {metrics['score']:.2f}% < {self.thresholds[dimension] * 100}%"
                }
                alerts.append(alert)
        
        # Save alerts
        if alerts:
            with open('monitoring/alerts/quality_alerts.json', 'w') as f:
                json.dump(alerts, f, indent=2)
            
            print(f"🚨 {len(alerts)} quality alerts generated!")
            for alert in alerts:
                print(f"  {alert['level']}: {alert['message']}")
        else:
            print("✅ No quality alerts - all metrics within thresholds!")
        
        return alerts
    
    def run_full_monitoring(self):
        """Run complete quality monitoring process"""
        print("🛡️ Starting Netflix Data Quality Monitoring")
        print("=" * 60)
        
        # Load data
        self.load_data()
        
        # Calculate all quality metrics
        print("📊 Calculating quality metrics...")
        overall_score, status = self.calculate_overall_quality_score()
        
        # Generate dashboard
        print("📈 Generating quality dashboard...")
        self.generate_quality_dashboard()
        
        # Save metrics
        print("💾 Saving quality metrics...")
        self.save_quality_metrics()
        
        # Generate alerts
        print("🚨 Checking for quality alerts...")
        alerts = self.generate_quality_alerts()
        
        # Print summary
        print("\n" + "="*60)
        print("🎯 QUALITY MONITORING SUMMARY")
        print("="*60)
        print(f"Overall Quality Score: {overall_score:.2f}%")
        print(f"Quality Status: {status}")
        print(f"Alerts Generated: {len(alerts)}")
        print("Dashboard saved to: monitoring/dashboards/quality_dashboard.png")
        print("="*60)
        
        return self.quality_metrics, alerts

def main():
    """Main execution function"""
    # Create monitoring directories if they don't exist
    import os
    os.makedirs('monitoring/dashboards', exist_ok=True)
    os.makedirs('monitoring/alerts', exist_ok=True)
    
    # Initialize and run monitoring
    monitor = DataQualityMonitor()
    quality_metrics, alerts = monitor.run_full_monitoring()
    
    return monitor, quality_metrics, alerts

if __name__ == "__main__":
    monitor, quality_metrics, alerts = main()
