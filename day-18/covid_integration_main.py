#!/usr/bin/env python3
"""
COVID-19 Data Integration System - Main Controller
Day 18: API Integration & External Data Sources

This is the main entry point for your COVID-19 data integration system.
It demonstrates all the concepts learned in Day 18.
"""

import sys
import os
import time
import webbrowser
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def print_banner():
    """Print system banner"""
    print("🌐" + "="*70 + "🌐")
    print("  COVID-19 DATA INTEGRATION SYSTEM")
    print("  Day 18: API Integration & External Data Sources")
    print("  Building Connected Data Ecosystems")
    print("🌐" + "="*70 + "🌐")

def show_menu():
    """Show main menu"""
    print("\n📋 What would you like to do?")
    print("1. 🧪 Test API Clients")
    print("2. 🔍 Run Health Checks")
    print("3. 🖥️  Show Monitoring Dashboard")
    print("4. 🚀 Start Automated Pipeline")
    print("5. 📊 Generate Integration Report")
    print("6. 🌐 Open Web Dashboard")
    print("7. 📚 Show Learning Summary")
    print("8. 🚪 Exit")
    print("-" * 50)

def test_api_clients():
    """Test the basic API clients"""
    print("\n🧪 Testing API Clients...")
    print("-" * 30)
    
    try:
        from src.apis.covid_clients import COVIDDataIntegrator
        
        integrator = COVIDDataIntegrator()
        
        # Test global data
        print("📊 Testing global COVID data...")
        global_summary = integrator.get_global_summary()
        print(f"✅ Global data from {len(global_summary['sources'])} sources")
        
        # Test country-specific data
        print("\n🌍 Testing country-specific data...")
        usa_data = integrator.get_comprehensive_country_data('USA')
        print(f"✅ USA data from {len(usa_data['sources'])} sources")
        
        print("\n✅ Basic API clients working!")
        
    except Exception as e:
        print(f"❌ Error testing API clients: {str(e)}")

def run_health_checks():
    """Run comprehensive health checks"""
    print("\n🔍 Running Health Checks...")
    print("-" * 30)
    
    try:
        from src.apis.monitored_covid_clients import ResilientCOVIDIntegrator
        
        integrator = ResilientCOVIDIntegrator()
        integrator.run_health_check()
        
        print("\n✅ Health checks completed!")
        
    except Exception as e:
        print(f"❌ Error running health checks: {str(e)}")

def show_monitoring_dashboard():
    """Show the monitoring dashboard"""
    print("\n🖥️  Monitoring Dashboard...")
    print("-" * 30)
    
    try:
        from src.apis.monitored_covid_clients import ResilientCOVIDIntegrator
        
        integrator = ResilientCOVIDIntegrator()
        integrator.monitor.print_dashboard()
        
    except Exception as e:
        print(f"❌ Error showing dashboard: {str(e)}")

def start_automated_pipeline():
    """Start the automated data pipeline"""
    print("\n🚀 Starting Automated Pipeline...")
    print("-" * 30)
    
    try:
        from src.pipeline.scheduler import DataPipelineScheduler
        
        scheduler = DataPipelineScheduler()
        
        print("⏰ Starting scheduler...")
        scheduler.start_scheduler()
        
        print("\n✅ Pipeline started! Running for 60 seconds...")
        print("💡 The pipeline will:")
        print("   • Update country data every 15 minutes")
        print("   • Monitor API health every 10 minutes")
        print("   • Export dashboard data every 30 minutes")
        print("   • Run comprehensive checks hourly")
        
        # Run for 1 minute in demo mode
        for i in range(12):  # 12 * 5 = 60 seconds
            time.sleep(5)
            print(f"⏱️  Running... {(i+1)*5}s")
        
        scheduler.stop_scheduler()
        print("\n✅ Pipeline demo completed!")
        
    except Exception as e:
        print(f"❌ Error starting pipeline: {str(e)}")

def generate_integration_report():
    """Generate comprehensive integration report"""
    print("\n📊 Generating Integration Report...")
    print("-" * 30)
    
    try:
        from src.apis.monitored_covid_clients import ResilientCOVIDIntegrator
        
        integrator = ResilientCOVIDIntegrator()
        
        # Test multiple countries
        countries = ['USA', 'India', 'Brazil', 'Germany']
        results = {}
        
        for country in countries:
            print(f"📡 Testing {country}...")
            try:
                data = integrator.get_comprehensive_country_data(country)
                results[country] = {
                    'sources': len(data['sources_successful']),
                    'success': True
                }
            except Exception as e:
                results[country] = {
                    'sources': 0,
                    'success': False,
                    'error': str(e)
                }
        
        # Generate report
        print("\n📋 INTEGRATION REPORT")
        print("=" * 40)
        print(f"🕐 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌍 Countries tested: {len(countries)}")
        
        successful = len([r for r in results.values() if r['success']])
        print(f"✅ Successful integrations: {successful}/{len(countries)}")
        
        total_sources = sum([r['sources'] for r in results.values()])
        max_sources = len(countries) * 3  # 3 sources per country
        print(f"📊 Source integration rate: {total_sources}/{max_sources} ({(total_sources/max_sources)*100:.1f}%)")
        
        print("\n📈 Detailed Results:")
        for country, result in results.items():
            if result['success']:
                print(f"   ✅ {country}: {result['sources']}/3 sources")
            else:
                print(f"   ❌ {country}: Failed - {result.get('error', 'Unknown error')}")
        
        # API health summary
        report = integrator.generate_monitoring_report()
        print(f"\n🏥 System Health: {report['overall_health']}")
        print("📊 API Performance:")
        for api_name, summary in report['api_summary'].items():
            print(f"   {api_name}: Grade {summary['performance_grade']} "
                  f"({summary['success_rate']*100:.1f}% success)")
        
        print("\n✅ Report generation completed!")
        
    except Exception as e:
        print(f"❌ Error generating report: {str(e)}")

def open_web_dashboard():
    """Open the web dashboard in browser"""
    print("\n🌐 Opening Web Dashboard...")
    print("-" * 30)
    
    dashboard_path = os.path.join(project_root, "dashboard.html")
    
    if os.path.exists(dashboard_path):
        try:
            webbrowser.open(f"file://{dashboard_path}")
            print("✅ Dashboard opened in your default browser!")
            print("💡 The dashboard shows:")
            print("   • Real-time COVID statistics")
            print("   • API health monitoring")
            print("   • System performance metrics")
            print("   • Interactive country selection")
        except Exception as e:
            print(f"❌ Could not open browser: {str(e)}")
            print(f"💡 Manually open: {dashboard_path}")
    else:
        print("❌ Dashboard file not found!")
        print("💡 Make sure you created dashboard.html with the provided code")

def show_learning_summary():
    """Show what was learned in Day 18"""
    print("\n📚 DAY 18 LEARNING SUMMARY")
    print("🎯 API Integration & External Data Sources")
    print("=" * 50)
    
    concepts = [
        ("🏗️ API Architecture Patterns", [
            "REST API design and consumption",
            "GraphQL query optimization",
            "Webhook event-driven architecture",
            "Authentication and authorization"
        ]),
        ("🔧 Integration Patterns", [
            "Circuit breaker pattern",
            "Retry logic with exponential backoff",
            "Graceful degradation strategies",
            "Multi-source data integration"
        ]),
        ("📊 Data Quality & Monitoring", [
            "Real-time API health monitoring",
            "Data quality assessment",
            "Performance metrics tracking",
            "Automated alerting systems"
        ]),
        ("🚀 Production Readiness", [
            "Automated data pipelines",
            "Resilient error handling",
            "Comprehensive logging",
            "Real-time dashboards"
        ]),
        ("🌐 External Data Sources", [
            "COVID-19 statistics (disease.sh)",
            "Country demographics (REST Countries)",
            "Economic indicators (World Bank)",
            "Multi-source data enrichment"
        ])
    ]
    
    for category, items in concepts:
        print(f"\n{category}:")
        for item in items:
            print(f"   ✅ {item}")
    
    print(f"\n🎊 Congratulations! You've built a production-ready")
    print(f"   data integration system with comprehensive monitoring!")
    
    print(f"\n🔗 Key Files Created:")
    files = [
        "src/apis/base_client.py - Robust API client framework",
        "src/apis/covid_clients.py - COVID-specific API clients", 
        "src/apis/monitored_covid_clients.py - Monitored clients with resilience",
        "src/monitoring/resilience_monitor.py - Comprehensive monitoring system",
        "src/pipeline/scheduler.py - Automated data pipeline",
        "dashboard.html - Real-time web dashboard",
        "config/config.py - Configuration management"
    ]
    
    for file_desc in files:
        print(f"   📄 {file_desc}")
    
    print(f"\n🚀 Next Steps:")
    next_steps = [
        "Deploy to cloud infrastructure (AWS, GCP, Azure)",
        "Add more external data sources (social media, news)",
        "Implement machine learning for predictive analytics",
        "Scale to handle enterprise-level data volumes",
        "Add advanced visualization with D3.js or Plotly"
    ]
    
    for step in next_steps:
        print(f"   🎯 {step}")

def main():
    """Main controller function"""
    print_banner()
    
    while True:
        show_menu()
        
        try:
            choice = input("Enter your choice (1-8): ").strip()
            
            if choice == '1':
                test_api_clients()
            elif choice == '2':
                run_health_checks()
            elif choice == '3':
                show_monitoring_dashboard()
            elif choice == '4':
                start_automated_pipeline()
            elif choice == '5':
                generate_integration_report()
            elif choice == '6':
                open_web_dashboard()
            elif choice == '7':
                show_learning_summary()
            elif choice == '8':
                print("\n👋 Thank you for using the COVID-19 Integration System!")
                print("🎓 Day 18 completed successfully!")
                break
            else:
                print("❌ Invalid choice. Please select 1-8.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ An error occurred: {str(e)}")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()
