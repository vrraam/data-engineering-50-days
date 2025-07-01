#!/usr/bin/env python3
"""
Quick Platform Health Check
===========================
Verify that all services are running properly
"""

import requests
import subprocess
import time
import json

def check_service_health():
    """Check if all services are healthy"""
    print("🔍 Checking platform health...")
    
    services = {
        'Airflow Web UI': 'http://localhost:8080',
        'Spark Master': 'http://localhost:8081',
        'Jupyter Lab': 'http://localhost:8888'
    }
    
    health_results = {}
    
    for service_name, url in services.items():
        try:
            print(f"🔄 Checking {service_name}...")
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service_name}: Healthy")
                health_results[service_name] = 'Healthy'
            else:
                print(f"⚠️ {service_name}: Responding but status {response.status_code}")
                health_results[service_name] = f'Status {response.status_code}'
        except Exception as e:
            print(f"❌ {service_name}: Not responding - {e}")
            health_results[service_name] = 'Not responding'
    
    return health_results

def check_docker_containers():
    """Check Docker container status"""
    print("\n🐳 Checking Docker containers...")
    
    try:
        result = subprocess.run(['docker-compose', 'ps'], 
                              capture_output=True, text=True, check=True)
        print(result.stdout)
        
        # Count running containers
        lines = result.stdout.strip().split('\n')[2:]  # Skip header lines
        running_containers = len([line for line in lines if 'Up' in line])
        total_containers = len(lines)
        
        print(f"📊 Containers running: {running_containers}/{total_containers}")
        return running_containers, total_containers
        
    except Exception as e:
        print(f"❌ Error checking containers: {e}")
        return 0, 0

def check_data_files():
    """Check if sample data files exist"""
    print("\n📊 Checking data files...")
    
    required_files = [
        'data/raw/ecommerce_transactions.csv',
        'data/raw/superstore_sales.csv',
        'data/raw/amazon_products.csv',
        'data/raw/retail_analytics.csv'
    ]
    
    existing_files = 0
    for file_path in required_files:
        try:
            with open(file_path, 'r') as f:
                lines = len(f.readlines())
            print(f"✅ {file_path}: {lines} lines")
            existing_files += 1
        except FileNotFoundError:
            print(f"❌ {file_path}: Not found")
        except Exception as e:
            print(f"⚠️ {file_path}: Error - {e}")
    
    print(f"📊 Data files available: {existing_files}/{len(required_files)}")
    return existing_files, len(required_files)

def main():
    """Main health check function"""
    print("🚀 Starting Platform Health Check...")
    print("=" * 50)
    
    # Check services
    service_health = check_service_health()
    
    # Check containers
    running_containers, total_containers = check_docker_containers()
    
    # Check data files
    available_files, total_files = check_data_files()
    
    # Overall assessment
    print("\n" + "=" * 50)
    print("📊 PLATFORM HEALTH SUMMARY")
    print("=" * 50)
    
    healthy_services = sum(1 for status in service_health.values() if status == 'Healthy')
    total_services = len(service_health)
    
    print(f"🌐 Web Services: {healthy_services}/{total_services} healthy")
    print(f"🐳 Docker Containers: {running_containers}/{total_containers} running")
    print(f"📊 Data Files: {available_files}/{total_files} available")
    
    # Calculate overall health score
    service_score = healthy_services / total_services if total_services > 0 else 0
    container_score = running_containers / total_containers if total_containers > 0 else 0
    data_score = available_files / total_files if total_files > 0 else 0
    
    overall_score = (service_score + container_score + data_score) / 3
    
    print(f"\n🎯 Overall Platform Health: {overall_score:.1%}")
    
    if overall_score >= 0.8:
        print("🎉 Platform is ready for use!")
        print("\n📋 Next Steps:")
        print("1. Access Airflow UI: http://localhost:8080 (airflow/airflow)")
        print("2. Access Spark UI: http://localhost:8081")
        print("3. Access Jupyter Lab: http://localhost:8888")
        print("4. Unpause and trigger the 'integrated_data_platform' DAG")
        return True
    else:
        print("⚠️ Platform needs attention before use")
        print("\n🔧 Troubleshooting:")
        print("1. Wait a few more minutes for services to fully start")
        print("2. Check logs: docker-compose logs [service-name]")
        print("3. Restart services: docker-compose restart")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)