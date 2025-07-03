import json
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import logging
import schedule
from collections import defaultdict, deque
import statistics
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class APIMetrics:
    """Metrics for API performance and health"""
    api_name: str
    timestamp: datetime
    response_time: float
    status_code: int
    success: bool
    error_message: Optional[str] = None
    data_quality_score: float = 1.0
    
    def to_dict(self):
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data

@dataclass
class HealthCheckResult:
    """Result of an API health check"""
    api_name: str
    status: HealthStatus
    response_time: float
    last_success: Optional[datetime]
    error_count: int
    success_rate: float
    message: str

class CircuitBreaker:
    """
    Circuit breaker pattern implementation
    Prevents cascade failures by stopping requests to failing services
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker is OPEN - too many failures")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if self.last_failure_time is None:
            return True
        return (datetime.now() - self.last_failure_time).seconds >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful request"""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def _on_failure(self):
        """Handle failed request"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
    
    def get_state(self) -> Dict:
        """Get current circuit breaker state"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure": self.last_failure_time.isoformat() if self.last_failure_time else None
        }

class APIHealthMonitor:
    """
    Comprehensive API health monitoring system
    Tracks performance, errors, and data quality
    """
    
    def __init__(self, db_path: str = "api_monitoring.db"):
        self.db_path = db_path
        self.circuit_breakers = {}
        self.metrics_buffer = deque(maxlen=1000)  # Keep last 1000 metrics
        self.alert_thresholds = {
            'error_rate': 0.1,  # 10% error rate
            'response_time': 5.0,  # 5 seconds
            'data_quality': 0.8   # 80% quality score
        }
        self._init_database()
        
    def _init_database(self):
        """Initialize SQLite database for storing metrics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_name TEXT,
                    timestamp TEXT,
                    response_time REAL,
                    status_code INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    data_quality_score REAL
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS health_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_name TEXT,
                    timestamp TEXT,
                    status TEXT,
                    response_time REAL,
                    error_count INTEGER,
                    success_rate REAL,
                    message TEXT
                )
            """)
    
    def record_api_call(self, api_name: str, response_time: float, 
                       status_code: int, success: bool, 
                       error_message: str = None, data_quality_score: float = 1.0):
        """Record metrics for an API call"""
        
        metric = APIMetrics(
            api_name=api_name,
            timestamp=datetime.now(),
            response_time=response_time,
            status_code=status_code,
            success=success,
            error_message=error_message,
            data_quality_score=data_quality_score
        )
        
        # Add to in-memory buffer
        self.metrics_buffer.append(metric)
        
        # Store in database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO api_metrics 
                (api_name, timestamp, response_time, status_code, success, error_message, data_quality_score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                metric.api_name, metric.timestamp.isoformat(), metric.response_time,
                metric.status_code, metric.success, metric.error_message, metric.data_quality_score
            ))
        
        # Check for alerts
        self._check_alerts(api_name)
    
    def get_circuit_breaker(self, api_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for an API"""
        if api_name not in self.circuit_breakers:
            self.circuit_breakers[api_name] = CircuitBreaker()
        return self.circuit_breakers[api_name]
    
    def health_check(self, api_name: str) -> HealthCheckResult:
        """Perform health check for an API"""
        
        # Get recent metrics (last hour)
        cutoff_time = datetime.now() - timedelta(hours=1)
        recent_metrics = [
            m for m in self.metrics_buffer 
            if m.api_name == api_name and m.timestamp > cutoff_time
        ]
        
        if not recent_metrics:
            return HealthCheckResult(
                api_name=api_name,
                status=HealthStatus.UNKNOWN,
                response_time=0.0,
                last_success=None,
                error_count=0,
                success_rate=0.0,
                message="No recent data"
            )
        
        # Calculate metrics
        total_calls = len(recent_metrics)
        successful_calls = len([m for m in recent_metrics if m.success])
        success_rate = successful_calls / total_calls if total_calls > 0 else 0
        avg_response_time = statistics.mean([m.response_time for m in recent_metrics])
        error_count = total_calls - successful_calls
        
        # Find last success
        last_success = None
        for metric in reversed(recent_metrics):
            if metric.success:
                last_success = metric.timestamp
                break
        
        # Determine health status
        status = HealthStatus.HEALTHY
        message = "API is healthy"
        
        if success_rate < (1 - self.alert_thresholds['error_rate']):
            status = HealthStatus.UNHEALTHY
            message = f"High error rate: {(1-success_rate)*100:.1f}%"
        elif avg_response_time > self.alert_thresholds['response_time']:
            status = HealthStatus.DEGRADED
            message = f"Slow response time: {avg_response_time:.2f}s"
        elif success_rate < 0.95:  # Less strict threshold for degraded
            status = HealthStatus.DEGRADED
            message = f"Moderate error rate: {(1-success_rate)*100:.1f}%"
        
        result = HealthCheckResult(
            api_name=api_name,
            status=status,
            response_time=avg_response_time,
            last_success=last_success,
            error_count=error_count,
            success_rate=success_rate,
            message=message
        )
        
        # Store health check result
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO health_checks 
                (api_name, timestamp, status, response_time, error_count, success_rate, message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                result.api_name, datetime.now().isoformat(), result.status.value,
                result.response_time, result.error_count, result.success_rate, result.message
            ))
        
        return result
    
    def _check_alerts(self, api_name: str):
        """Check if any alerts should be triggered"""
        
        # Get recent metrics for this API (last 10 minutes)
        cutoff_time = datetime.now() - timedelta(minutes=10)
        recent_metrics = [
            m for m in self.metrics_buffer 
            if m.api_name == api_name and m.timestamp > cutoff_time
        ]
        
        if len(recent_metrics) < 5:  # Need at least 5 data points
            return
        
        # Check error rate
        error_rate = len([m for m in recent_metrics if not m.success]) / len(recent_metrics)
        if error_rate > self.alert_thresholds['error_rate']:
            self._send_alert(f"🚨 HIGH ERROR RATE: {api_name} - {error_rate*100:.1f}% errors")
        
        # Check response time
        avg_response_time = statistics.mean([m.response_time for m in recent_metrics])
        if avg_response_time > self.alert_thresholds['response_time']:
            self._send_alert(f"🐌 SLOW RESPONSE: {api_name} - {avg_response_time:.2f}s average")
        
        # Check data quality
        avg_quality = statistics.mean([m.data_quality_score for m in recent_metrics])
        if avg_quality < self.alert_thresholds['data_quality']:
            self._send_alert(f"📉 LOW DATA QUALITY: {api_name} - {avg_quality*100:.1f}% quality score")
    
    def _send_alert(self, message: str):
        """Send alert (for now, just log it)"""
        logging.warning(f"ALERT: {message}")
        print(f"🚨 ALERT: {message}")
    
    def get_dashboard_data(self) -> Dict:
        """Get data for monitoring dashboard"""
        
        # Get health status for all APIs
        api_names = list(set([m.api_name for m in self.metrics_buffer]))
        health_statuses = {}
        
        for api_name in api_names:
            health_statuses[api_name] = self.health_check(api_name)
        
        # Calculate overall system health
        healthy_apis = len([h for h in health_statuses.values() if h.status == HealthStatus.HEALTHY])
        total_apis = len(health_statuses)
        overall_health = "HEALTHY" if healthy_apis == total_apis else "DEGRADED"
        
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_health": overall_health,
            "healthy_apis": healthy_apis,
            "total_apis": total_apis,
            "api_health": {name: {
                "status": health.status.value,
                "success_rate": health.success_rate,
                "response_time": health.response_time,
                "message": health.message
            } for name, health in health_statuses.items()},
            "circuit_breakers": {name: cb.get_state() for name, cb in self.circuit_breakers.items()}
        }
    
    def print_dashboard(self):
        """Print a simple text dashboard"""
        dashboard_data = self.get_dashboard_data()
        
        print("\n" + "="*60)
        print("🖥️  API MONITORING DASHBOARD")
        print("="*60)
        print(f"📊 Overall System Health: {dashboard_data['overall_health']}")
        print(f"✅ Healthy APIs: {dashboard_data['healthy_apis']}/{dashboard_data['total_apis']}")
        print(f"🕐 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n📡 API Health Status:")
        for api_name, health in dashboard_data['api_health'].items():
            status_emoji = {
                'healthy': '✅',
                'degraded': '⚠️',
                'unhealthy': '❌',
                'unknown': '❓'
            }
            emoji = status_emoji.get(health['status'], '❓')
            print(f"   {emoji} {api_name}:")
            print(f"      Status: {health['status'].upper()}")
            print(f"      Success Rate: {health['success_rate']*100:.1f}%")
            print(f"      Avg Response: {health['response_time']:.2f}s")
            print(f"      Message: {health['message']}")
        
        print("\n🔌 Circuit Breakers:")
        for api_name, cb_state in dashboard_data['circuit_breakers'].items():
            state_emoji = {'CLOSED': '🟢', 'OPEN': '🔴', 'HALF_OPEN': '🟡'}
            emoji = state_emoji.get(cb_state['state'], '❓')
            print(f"   {emoji} {api_name}: {cb_state['state']} ({cb_state['failure_count']} failures)")
        
        print("="*60)

# Test the monitoring system
if __name__ == "__main__":
    print("🧪 Testing API Resilience and Monitoring System")
    print("=" * 50)
    
    # Create monitor
    monitor = APIHealthMonitor("test_monitoring.db")
    
    # Simulate some API calls
    print("\n📊 Simulating API calls...")
    
    # Simulate successful calls
    for i in range(10):
        monitor.record_api_call("disease.sh", 0.5 + i*0.1, 200, True, data_quality_score=0.95)
        time.sleep(0.1)
    
    # Simulate some failures
    for i in range(3):
        monitor.record_api_call("disease.sh", 2.0, 500, False, "Server Error", data_quality_score=0.0)
        time.sleep(0.1)
    
    # Simulate different API
    for i in range(5):
        monitor.record_api_call("worldbank", 1.0 + i*0.2, 200, True, data_quality_score=0.9)
        time.sleep(0.1)
    
    # Test circuit breaker
    print("\n🔌 Testing Circuit Breaker...")
    cb = monitor.get_circuit_breaker("test_api")
    
    def failing_function():
        raise Exception("API is down!")
    
    # Try to call failing function multiple times
    for i in range(7):
        try:
            cb.call(failing_function)
        except Exception as e:
            print(f"   Attempt {i+1}: {str(e)}")
    
    print(f"   Circuit Breaker State: {cb.get_state()}")
    
    # Show dashboard
    print("\n🖥️  Monitoring Dashboard:")
    monitor.print_dashboard()
    
    print("\n✅ Resilience and monitoring system testing complete!")
    print("\n🎯 Next: We'll integrate this with our COVID API clients!")
