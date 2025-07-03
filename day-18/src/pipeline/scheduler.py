import sys
import os
import json
import time
import schedule
import threading
from datetime import datetime, timedelta
from typing import Dict, List
import sqlite3
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from src.apis.monitored_covid_clients import ResilientCOVIDIntegrator
    from src.monitoring.resilience_monitor import APIHealthMonitor
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure all previous files are created and accessible")
    exit(1)

class DataPipelineScheduler:
    """
    Automated scheduler for COVID data integration pipeline
    Handles periodic data updates, health checks, and dashboard data generation
    """
    
    def __init__(self, db_path: str = "dashboard_data.db"):
        self.db_path = db_path
        self.integrator = ResilientCOVIDIntegrator()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
        self.scheduler_thread = None
        
        # Countries to monitor
        self.monitored_countries = [
            'USA', 'India', 'Brazil', 'Germany', 'France', 
            'UK', 'Italy', 'Spain', 'Canada', 'Australia'
        ]
        
        self._init_dashboard_database()
        
    def _init_dashboard_database(self):
        """Initialize database for dashboard data"""
        with sqlite3.connect(self.db_path) as conn:
            # Table for latest country data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS country_data (
                    country TEXT PRIMARY KEY,
                    last_updated TEXT,
                    covid_cases INTEGER,
                    covid_deaths INTEGER,
                    covid_recovered INTEGER,
                    population INTEGER,
                    data_sources TEXT,
                    data_quality_score REAL
                )
            """)
            
            # Table for API performance metrics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    api_name TEXT,
                    success_rate REAL,
                    avg_response_time REAL,
                    status TEXT,
                    grade TEXT
                )
            """)
            
            # Table for system alerts
            conn.execute("""
                CREATE TABLE IF NOT EXISTS system_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    alert_type TEXT,
                    message TEXT,
                    resolved BOOLEAN DEFAULT FALSE
                )
            """)
    
    def update_country_data(self, country: str):
        """Update data for a specific country"""
        self.logger.info(f"🔄 Updating data for {country}")
        
        try:
            # Get comprehensive data
            data = self.integrator.get_comprehensive_country_data(country)
            
            # Extract key metrics
            covid_cases = 0
            covid_deaths = 0
            covid_recovered = 0
            population = 0
            data_quality_score = 0.0
            
            if 'covid_stats' in data:
                covid_data = data['covid_stats']['data']
                covid_cases = covid_data.get('cases', 0)
                covid_deaths = covid_data.get('deaths', 0)
                covid_recovered = covid_data.get('recovered', 0)
            
            if 'demographics' in data:
                demo_data = data['demographics']['data']
                population = demo_data.get('population', 0)
            
            # Calculate overall data quality score
            source_count = len(data['sources_successful'])
            max_sources = 3  # We have 3 possible sources
            data_quality_score = source_count / max_sources
            
            # Store in database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO country_data 
                    (country, last_updated, covid_cases, covid_deaths, covid_recovered, 
                     population, data_sources, data_quality_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    country, datetime.now().isoformat(), covid_cases, covid_deaths,
                    covid_recovered, population, json.dumps(data['sources_successful']),
                    data_quality_score
                ))
            
            self.logger.info(f"✅ Updated {country}: {covid_cases:,} cases, {source_count}/3 sources")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update {country}: {str(e)}")
            
            # Store alert
            self._add_alert('error', f"Failed to update data for {country}: {str(e)}")
    
    def update_all_countries(self):
        """Update data for all monitored countries"""
        self.logger.info(f"🌍 Starting bulk update for {len(self.monitored_countries)} countries")
        
        start_time = time.time()
        successful_updates = 0
        
        for country in self.monitored_countries:
            try:
                self.update_country_data(country)
                successful_updates += 1
                # Small delay between requests to be respectful to APIs
                time.sleep(2)
            except Exception as e:
                self.logger.error(f"❌ Failed to update {country}: {str(e)}")
        
        duration = time.time() - start_time
        self.logger.info(f"✅ Bulk update complete: {successful_updates}/{len(self.monitored_countries)} "
                        f"countries updated in {duration:.1f}s")
        
        if successful_updates < len(self.monitored_countries):
            self._add_alert('warning', 
                          f"Only {successful_updates}/{len(self.monitored_countries)} countries updated successfully")
    
    def update_api_performance_metrics(self):
        """Update API performance metrics for dashboard"""
        self.logger.info("📊 Updating API performance metrics")
        
        # Get current API health data
        dashboard_data = self.integrator.monitor.get_dashboard_data()
        
        timestamp = datetime.now().isoformat()
        
        # Store performance metrics
        with sqlite3.connect(self.db_path) as conn:
            for api_name, health_data in dashboard_data['api_health'].items():
                # Calculate grade based on performance
                success_rate = health_data['success_rate']
                response_time = health_data['response_time']
                
                if success_rate >= 0.98 and response_time < 2.0:
                    grade = 'A'
                elif success_rate >= 0.95 and response_time < 3.0:
                    grade = 'B'
                elif success_rate >= 0.90:
                    grade = 'C'
                else:
                    grade = 'D'
                
                conn.execute("""
                    INSERT INTO api_performance 
                    (timestamp, api_name, success_rate, avg_response_time, status, grade)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    timestamp, api_name, success_rate, response_time,
                    health_data['status'], grade
                ))
        
        self.logger.info("✅ API performance metrics updated")
    
    def run_health_checks(self):
        """Run comprehensive health checks"""
        self.logger.info("🔍 Running scheduled health checks")
        
        # Run health check through integrator
        self.integrator.run_health_check()
        
        # Check for any critical issues
        dashboard_data = self.integrator.monitor.get_dashboard_data()
        
        for api_name, health_data in dashboard_data['api_health'].items():
            if health_data['status'] == 'unhealthy':
                self._add_alert('error', f"API {api_name} is unhealthy: {health_data['message']}")
            elif health_data['status'] == 'degraded':
                self._add_alert('warning', f"API {api_name} performance degraded: {health_data['message']}")
        
        # Check circuit breaker states
        for api_name, cb_state in dashboard_data['circuit_breakers'].items():
            if cb_state['state'] == 'OPEN':
                self._add_alert('error', f"Circuit breaker OPEN for {api_name}")
        
        self.logger.info("✅ Health checks completed")
    
    def _add_alert(self, alert_type: str, message: str):
        """Add system alert"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO system_alerts (timestamp, alert_type, message)
                VALUES (?, ?, ?)
            """, (datetime.now().isoformat(), alert_type, message))
        
        self.logger.warning(f"🚨 Alert [{alert_type.upper()}]: {message}")
    
    def get_dashboard_data(self) -> Dict:
        """Get all data needed for dashboard"""
        with sqlite3.connect(self.db_path) as conn:
            # Get latest country data
            countries_data = {}
            cursor = conn.execute("""
                SELECT country, last_updated, covid_cases, covid_deaths, covid_recovered, 
                       population, data_sources, data_quality_score
                FROM country_data
                ORDER BY last_updated DESC
            """)
            
            for row in cursor.fetchall():
                countries_data[row[0]] = {
                    'country': row[0],
                    'last_updated': row[1],
                    'covid_cases': row[2],
                    'covid_deaths': row[3],
                    'covid_recovered': row[4],
                    'population': row[5],
                    'data_sources': json.loads(row[6]) if row[6] else [],
                    'data_quality_score': row[7]
                }
            
            # Get latest API performance
            api_performance = {}
            cursor = conn.execute("""
                SELECT api_name, success_rate, avg_response_time, status, grade
                FROM api_performance
                WHERE timestamp = (
                    SELECT MAX(timestamp) FROM api_performance
                )
            """)
            
            for row in cursor.fetchall():
                api_performance[row[0]] = {
                    'success_rate': row[1],
                    'avg_response_time': row[2],
                    'status': row[3],
                    'grade': row[4]
                }
            
            # Get recent alerts
            alerts = []
            cursor = conn.execute("""
                SELECT timestamp, alert_type, message
                FROM system_alerts
                WHERE datetime(timestamp) > datetime('now', '-24 hours')
                AND resolved = FALSE
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            
            for row in cursor.fetchall():
                alerts.append({
                    'timestamp': row[0],
                    'type': row[1],
                    'message': row[2]
                })
        
        return {
            'countries': countries_data,
            'api_performance': api_performance,
            'alerts': alerts,
            'last_updated': datetime.now().isoformat()
        }
    
    def export_dashboard_json(self, filepath: str = "dashboard_data.json"):
        """Export dashboard data to JSON file"""
        dashboard_data = self.get_dashboard_data()
        
        with open(filepath, 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
        self.logger.info(f"📄 Dashboard data exported to {filepath}")
        return filepath
    
    def setup_schedule(self):
        """Setup the automated schedule"""
        self.logger.info("⏰ Setting up automated schedule")
        
        # Every 15 minutes: Update high-priority countries
        schedule.every(15).minutes.do(self.update_priority_countries)
        
        # Every hour: Update all countries
        schedule.every().hour.do(self.update_all_countries)
        
        # Every 5 minutes: Update API performance metrics
        schedule.every(5).minutes.do(self.update_api_performance_metrics)
        
        # Every 10 minutes: Run health checks
        schedule.every(10).minutes.do(self.run_health_checks)
        
        # Every 30 minutes: Export dashboard data
        schedule.every(30).minutes.do(self.export_dashboard_json)
        
        # Daily at 6 AM: Comprehensive system check
        schedule.every().day.at("06:00").do(self.daily_maintenance)
        
        self.logger.info("✅ Schedule configured:")
        self.logger.info("   • Priority countries: Every 15 minutes")
        self.logger.info("   • All countries: Every hour")
        self.logger.info("   • API metrics: Every 5 minutes")
        self.logger.info("   • Health checks: Every 10 minutes")
        self.logger.info("   • Dashboard export: Every 30 minutes")
        self.logger.info("   • Daily maintenance: 6:00 AM")
    
    def update_priority_countries(self):
        """Update high-priority countries more frequently"""
        priority_countries = ['USA', 'India', 'Brazil', 'Germany', 'France']
        
        self.logger.info(f"🚀 Priority update for {len(priority_countries)} countries")
        
        for country in priority_countries:
            try:
                self.update_country_data(country)
                time.sleep(1)  # Shorter delay for priority updates
            except Exception as e:
                self.logger.error(f"❌ Priority update failed for {country}: {str(e)}")
    
    def daily_maintenance(self):
        """Run daily maintenance tasks"""
        self.logger.info("🔧 Running daily maintenance")
        
        # Clean up old data (keep last 30 days)
        cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            # Clean up old API performance data
            conn.execute("DELETE FROM api_performance WHERE timestamp < ?", (cutoff_date,))
            
            # Mark old alerts as resolved
            conn.execute("UPDATE system_alerts SET resolved = TRUE WHERE timestamp < ?", (cutoff_date,))
        
        # Generate comprehensive report
        report = self.generate_daily_report()
        self.logger.info(f"📊 Daily report generated: {report['summary']}")
        
        self.logger.info("✅ Daily maintenance completed")
    
    def generate_daily_report(self) -> Dict:
        """Generate daily system report"""
        dashboard_data = self.get_dashboard_data()
        
        total_countries = len(dashboard_data['countries'])
        recent_updates = len([c for c in dashboard_data['countries'].values() 
                            if (datetime.now() - datetime.fromisoformat(c['last_updated'])).seconds < 3600])
        
        avg_quality = sum([c['data_quality_score'] for c in dashboard_data['countries'].values()]) / total_countries if total_countries > 0 else 0
        
        healthy_apis = len([api for api in dashboard_data['api_performance'].values() 
                          if api['status'] == 'healthy'])
        total_apis = len(dashboard_data['api_performance'])
        
        report = {
            'date': datetime.now().date().isoformat(),
            'summary': f"{recent_updates}/{total_countries} countries updated, {healthy_apis}/{total_apis} APIs healthy",
            'countries_monitored': total_countries,
            'countries_updated_recently': recent_updates,
            'average_data_quality': avg_quality,
            'healthy_apis': healthy_apis,
            'total_apis': total_apis,
            'active_alerts': len(dashboard_data['alerts'])
        }
        
        return report
    
    def start_scheduler(self):
        """Start the automated scheduler"""
        if self.is_running:
            self.logger.warning("⚠️ Scheduler is already running")
            return
        
        self.setup_schedule()
        self.is_running = True
        
        def run_scheduler():
            self.logger.info("🚀 Starting automated data pipeline scheduler")
            
            # Run initial data collection
            self.logger.info("📊 Running initial data collection...")
            self.update_priority_countries()
            self.update_api_performance_metrics()
            self.run_health_checks()
            self.export_dashboard_json()
            
            # Start the schedule loop
            while self.is_running:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
        
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("✅ Scheduler started successfully")
    
    def stop_scheduler(self):
        """Stop the automated scheduler"""
        if not self.is_running:
            self.logger.warning("⚠️ Scheduler is not running")
            return
        
        self.is_running = False
        schedule.clear()
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        self.logger.info("🛑 Scheduler stopped")
    
    def show_status(self):
        """Show current scheduler status"""
        print("\n🖥️  DATA PIPELINE SCHEDULER STATUS")
        print("=" * 50)
        print(f"Running: {'✅ Yes' if self.is_running else '❌ No'}")
        
        if self.is_running:
            print(f"Scheduled Jobs: {len(schedule.jobs)}")
            print("\n📅 Next Scheduled Jobs:")
            for job in schedule.jobs[:5]:  # Show next 5 jobs
                print(f"   • {job.next_run.strftime('%H:%M:%S')} - {job.job_func.__name__}")
        
        # Show recent data
        dashboard_data = self.get_dashboard_data()
        print(f"\n📊 Data Status:")
        print(f"   Countries monitored: {len(dashboard_data['countries'])}")
        print(f"   APIs tracked: {len(dashboard_data['api_performance'])}")
        print(f"   Active alerts: {len(dashboard_data['alerts'])}")
        print(f"   Last updated: {dashboard_data['last_updated']}")

# Demo and testing
if __name__ == "__main__":
    print("🧪 Testing Data Pipeline Scheduler")
    print("=" * 50)
    
    # Create scheduler
    scheduler = DataPipelineScheduler("test_dashboard.db")
    
    try:
        # Test individual components
        print("\n📊 Testing country data update...")
        scheduler.update_country_data('USA')
        
        print("\n📊 Testing API performance metrics...")
        scheduler.update_api_performance_metrics()
        
        print("\n🔍 Testing health checks...")
        scheduler.run_health_checks()
        
        print("\n📄 Testing data export...")
        scheduler.export_dashboard_json("test_dashboard_data.json")
        
        print("\n📋 Testing dashboard data retrieval...")
        dashboard_data = scheduler.get_dashboard_data()
        print(f"✅ Retrieved data for {len(dashboard_data['countries'])} countries")
        print(f"✅ API performance data: {len(dashboard_data['api_performance'])} APIs")
        print(f"✅ System alerts: {len(dashboard_data['alerts'])} active")
        
        # Show status
        scheduler.show_status()
        
        # Test scheduler (run for 2 minutes in demo mode)
        print("\n🚀 Starting scheduler for demo (30 seconds)...")
        scheduler.start_scheduler()
        
        # Let it run for a bit
        time.sleep(30)
        
        print("\n📊 Showing status after running...")
        scheduler.show_status()
        
        # Stop scheduler
        scheduler.stop_scheduler()
        
        print("\n✅ Scheduler testing complete!")
        print("\n🎯 Dashboard files created:")
        print(f"   • Database: {scheduler.db_path}")
        print(f"   • JSON export: test_dashboard_data.json")
        print(f"   • HTML dashboard: Open the dashboard.html file in a browser!")
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping scheduler...")
        scheduler.stop_scheduler()
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
