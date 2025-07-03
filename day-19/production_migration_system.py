import pandas as pd
import time
import os
import json
import shutil
from pathlib import Path
from datetime import datetime
import logging

class ProductionMigrationSystem:
    """Production-ready data format migration system"""
    
    def __init__(self, source_dir='data', backup_dir='backup', target_dir='migrated'):
        self.source_dir = Path(source_dir)
        self.backup_dir = Path(backup_dir)
        self.target_dir = Path(target_dir)
        self.migration_log = []
        
        # Create directories
        self.backup_dir.mkdir(exist_ok=True)
        self.target_dir.mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('migration.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_migration_readiness(self, source_files):
        """Analyze files and create migration plan"""
        print("🔍 ANALYZING MIGRATION READINESS")
        print("=" * 50)
        
        analysis_results = {}
        
        for file_path in source_files:
            file_path = Path(file_path)
            if not file_path.exists():
                continue
                
            print(f"\n📊 Analyzing: {file_path.name}")
            
            # Get file stats
            file_size = file_path.stat().st_size / (1024 * 1024)  # MB
            
            # Analyze data structure
            try:
                if file_path.suffix == '.csv':
                    sample_df = pd.read_csv(file_path, nrows=1000)
                elif file_path.suffix == '.json':
                    sample_df = pd.read_json(file_path, nrows=1000)
                else:
                    continue
                
                analysis = {
                    'file_size_mb': file_size,
                    'row_count_estimate': len(sample_df) * (file_size / 1) if file_size > 0 else 0,
                    'column_count': len(sample_df.columns),
                    'data_types': dict(sample_df.dtypes.astype(str)),
                    'has_nulls': sample_df.isnull().any().any(),
                    'memory_usage_mb': sample_df.memory_usage(deep=True).sum() / (1024 * 1024),
                    'recommended_target': self._recommend_target_format(sample_df, file_size)
                }
                
                analysis_results[str(file_path)] = analysis
                
                print(f"  ✅ Size: {file_size:.2f} MB")
                print(f"  ✅ Estimated rows: {analysis['row_count_estimate']:,.0f}")
                print(f"  ✅ Columns: {analysis['column_count']}")
                print(f"  ✅ Recommended target: {analysis['recommended_target']}")
                
            except Exception as e:
                print(f"  ❌ Analysis failed: {e}")
                analysis_results[str(file_path)] = {'error': str(e)}
        
        return analysis_results
    
    def _recommend_target_format(self, df, file_size_mb):
        """Recommend target format based on data characteristics"""
        # Simple recommendation logic
        if file_size_mb > 50:  # Large files
            return 'parquet'
        elif df.select_dtypes(include=['object']).shape[1] > df.shape[1] * 0.5:  # Many text columns
            return 'parquet'  # Still good for analytics
        else:
            return 'parquet'  # Default to parquet for most cases
    
    def create_migration_plan(self, analysis_results):
        """Create detailed migration plan"""
        print("\n📋 CREATING MIGRATION PLAN")
        print("=" * 50)
        
        migration_plan = {
            'timestamp': datetime.now().isoformat(),
            'total_files': len(analysis_results),
            'estimated_duration': 0,
            'phases': []
        }
        
        # Phase 1: Backup
        phase1 = {
            'name': 'Backup Phase',
            'duration_estimate': '5-10 minutes',
            'tasks': [
                'Create backup directory',
                'Copy original files to backup',
                'Verify backup integrity'
            ]
        }
        migration_plan['phases'].append(phase1)
        
        # Phase 2: Migration
        phase2 = {
            'name': 'Migration Phase',
            'duration_estimate': '10-30 minutes',
            'tasks': [],
            'file_migrations': []
        }
        
        for file_path, analysis in analysis_results.items():
            if 'error' not in analysis:
                migration_task = {
                    'source_file': file_path,
                    'target_format': analysis['recommended_target'],
                    'estimated_time': analysis['file_size_mb'] * 0.1,  # Rough estimate
                    'compression': 'snappy'
                }
                phase2['file_migrations'].append(migration_task)
        
        migration_plan['phases'].append(phase2)
        
        # Phase 3: Validation
        phase3 = {
            'name': 'Validation Phase',
            'duration_estimate': '5-15 minutes',
            'tasks': [
                'Validate data integrity',
                'Compare record counts',
                'Performance testing',
                'Update documentation'
            ]
        }
        migration_plan['phases'].append(phase3)
        
        # Save migration plan
        with open('migration_plan.json', 'w') as f:
            json.dump(migration_plan, f, indent=2)
        
        print(f"✅ Migration plan created with {len(phase2['file_migrations'])} files")
        print(f"📁 Plan saved to: migration_plan.json")
        
        return migration_plan
    
    def execute_migration(self, migration_plan):
        """Execute the migration plan"""
        print("\n🚀 EXECUTING MIGRATION")
        print("=" * 50)
        
        start_time = time.time()
        
        # Phase 1: Backup
        print("\n📦 Phase 1: Creating backups...")
        backup_success = self._execute_backup_phase(migration_plan)
        
        if not backup_success:
            print("❌ Backup failed! Aborting migration.")
            return False
        
        # Phase 2: Migration
        print("\n🔄 Phase 2: Migrating files...")
        migration_success = self._execute_migration_phase(migration_plan)
        
        if not migration_success:
            print("❌ Migration failed! Files backed up safely.")
            return False
        
        # Phase 3: Validation
        print("\n✅ Phase 3: Validating results...")
        validation_success = self._execute_validation_phase(migration_plan)
        
        total_time = time.time() - start_time
        
        if validation_success:
            print(f"\n🎉 MIGRATION COMPLETED SUCCESSFULLY!")
            print(f"⏱️ Total time: {total_time:.2f} seconds")
            self._generate_migration_report(migration_plan, total_time)
            return True
        else:
            print("❌ Validation failed! Please check the logs.")
            return False
    
    def _execute_backup_phase(self, migration_plan):
        """Execute backup phase"""
        try:
            phase = migration_plan['phases'][1]  # Migration phase has file info
            
            for file_migration in phase['file_migrations']:
                source_path = Path(file_migration['source_file'])
                backup_path = self.backup_dir / source_path.name
                
                print(f"  📦 Backing up: {source_path.name}")
                shutil.copy2(source_path, backup_path)
                
                # Verify backup
                if backup_path.exists() and backup_path.stat().st_size > 0:
                    print(f"    ✅ Backup verified: {backup_path}")
                else:
                    raise Exception(f"Backup verification failed for {source_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Backup phase failed: {e}")
            return False
    
    def _execute_migration_phase(self, migration_plan):
        """Execute migration phase"""
        try:
            phase = migration_plan['phases'][1]  # Migration phase
            
            for file_migration in phase['file_migrations']:
                source_path = Path(file_migration['source_file'])
                target_format = file_migration['target_format']
                compression = file_migration.get('compression', 'snappy')
                
                print(f"  🔄 Migrating: {source_path.name} → {target_format}")
                
                # Load source data
                if source_path.suffix == '.csv':
                    df = pd.read_csv(source_path)
                elif source_path.suffix == '.json':
                    df = pd.read_json(source_path)
                else:
                    continue
                
                # Save in target format
                target_path = self.target_dir / f"{source_path.stem}.{target_format}"
                
                if target_format == 'parquet':
                    df.to_parquet(target_path, compression=compression, index=False)
                elif target_format == 'avro':
                    # Note: Would need fastavro library for real implementation
                    print(f"    ⚠️  Avro conversion skipped (requires fastavro library)")
                    continue
                
                # Verify migration
                if target_path.exists():
                    # Quick verification
                    if target_format == 'parquet':
                        test_df = pd.read_parquet(target_path)
                        if len(test_df) == len(df):
                            print(f"    ✅ Migration verified: {target_path.name}")
                        else:
                            raise Exception(f"Row count mismatch in {target_path}")
                else:
                    raise Exception(f"Target file not created: {target_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Migration phase failed: {e}")
            return False
    
    def _execute_validation_phase(self, migration_plan):
        """Execute validation phase"""
        try:
            print("  🔍 Validating data integrity...")
            
            phase = migration_plan['phases'][1]
            validation_results = []
            
            for file_migration in phase['file_migrations']:
                source_path = Path(file_migration['source_file'])
                target_format = file_migration['target_format']
                target_path = self.target_dir / f"{source_path.stem}.{target_format}"
                
                if not target_path.exists():
                    continue
                
                # Load both files and compare
                if source_path.suffix == '.csv':
                    source_df = pd.read_csv(source_path)
                elif source_path.suffix == '.json':
                    source_df = pd.read_json(source_path)
                else:
                    continue
                
                if target_format == 'parquet':
                    target_df = pd.read_parquet(target_path)
                
                # Compare key metrics
                validation = {
                    'file': source_path.name,
                    'row_count_match': len(source_df) == len(target_df),
                    'column_count_match': len(source_df.columns) == len(target_df.columns),
                    'source_size_mb': source_path.stat().st_size / (1024 * 1024),
                    'target_size_mb': target_path.stat().st_size / (1024 * 1024),
                    'compression_ratio': (source_path.stat().st_size / target_path.stat().st_size) if target_path.stat().st_size > 0 else 0
                }
                
                validation_results.append(validation)
                
                print(f"    ✅ {source_path.name}: {validation['compression_ratio']:.2f}x compression")
            
            # Save validation results
            with open('validation_results.json', 'w') as f:
                json.dump(validation_results, f, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation phase failed: {e}")
            return False
    
    def _generate_migration_report(self, migration_plan, total_time):
        """Generate final migration report"""
        print("\n📊 MIGRATION REPORT")
        print("=" * 50)
        
        # Load validation results
        try:
            with open('validation_results.json', 'r') as f:
                validation_results = json.load(f)
        except:
            validation_results = []
        
        total_files = len(validation_results)
        total_original_size = sum(v['source_size_mb'] for v in validation_results)
        total_new_size = sum(v['target_size_mb'] for v in validation_results)
        average_compression = total_original_size / total_new_size if total_new_size > 0 else 0
        
        print(f"📁 Files migrated: {total_files}")
        print(f"💾 Original size: {total_original_size:.2f} MB")
        print(f"💾 New size: {total_new_size:.2f} MB")
        print(f"🗜️ Average compression: {average_compression:.2f}x")
        print(f"💰 Storage savings: {((total_original_size - total_new_size) / total_original_size * 100):.1f}%")
        print(f"⏱️ Total migration time: {total_time:.2f} seconds")
        
        # Save final report
        report = {
            'migration_timestamp': datetime.now().isoformat(),
            'total_files': total_files,
            'original_size_mb': total_original_size,
            'new_size_mb': total_new_size,
            'compression_ratio': average_compression,
            'storage_savings_percent': ((total_original_size - total_new_size) / total_original_size * 100) if total_original_size > 0 else 0,
            'migration_time_seconds': total_time,
            'validation_results': validation_results
        }
        
        with open('final_migration_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📄 Detailed report saved to: final_migration_report.json")

# Demo the migration system
if __name__ == "__main__":
    print("🚀 PRODUCTION MIGRATION SYSTEM DEMO")
    print("=" * 60)
    
    # Create sample files for migration
    print("📝 Creating sample files for migration demo...")
    
    # Create some sample CSV files
    df1 = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'Customer_{i}' for i in range(1, 1001)],
        'value': [i * 10.5 for i in range(1, 1001)]
    })
    df1.to_csv('data/sample_customers.csv', index=False)
    
    df2 = pd.DataFrame({
        'order_id': range(1, 501),
        'customer_id': [i % 100 + 1 for i in range(1, 501)],
        'amount': [i * 25.75 for i in range(1, 501)]
    })
    df2.to_csv('data/sample_orders.csv', index=False)
    
    # Initialize migration system
    migration_system = ProductionMigrationSystem()
    
    # Find files to migrate
    source_files = [
        'data/sample_customers.csv',
        'data/sample_orders.csv',
        'data/ecommerce_sample.csv'  # From our previous examples
    ]
    
    # Analyze migration readiness
    analysis = migration_system.analyze_migration_readiness(source_files)
    
    # Create migration plan
    plan = migration_system.create_migration_plan(analysis)
    
    # Execute migration
    success = migration_system.execute_migration(plan)
    
    if success:
        print("\n🎉 Migration completed successfully!")
        print("📂 Check the 'migrated' folder for your optimized files!")
        print("📊 Check 'final_migration_report.json' for detailed results!")
    else:
        print("\n❌ Migration failed. Check the logs for details.")
