import sys
import os
import time
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import our modules
try:
    from src.apis.base_client import BaseAPIClient, APIError
    from src.monitoring.resilience_monitor import APIHealthMonitor
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback imports would go here
    exit(1)

import logging
from typing import Dict, List, Optional

class MonitoredAPIClient(BaseAPIClient):
    """
    Base API client with integrated monitoring and resilience
    """
    
    def __init__(self, base_url: str, api_key: str = None, timeout: int = 30, monitor: APIHealthMonitor = None):
        super().__init__(base_url, api_key, timeout)
        self.monitor = monitor or APIHealthMonitor()
        self.api_name = self.__class__.__name__.replace('Client', '').lower()
        
    def _make_monitored_request(self, method: str, endpoint: str, **kwargs):
        """Make request with monitoring and resilience"""
        
        start_time = time.time()
        circuit_breaker = self.monitor.get_circuit_breaker(self.api_name)
        
        try:
            # Use circuit breaker
            response = circuit_breaker.call(
                super()._make_request,
                method, endpoint, **kwargs
            )
            
            # Record successful call
            response_time = time.time() - start_time
            data_quality = self._assess_data_quality(response)
            
            self.monitor.record_api_call(
                api_name=self.api_name,
                response_time=response_time,
                status_code=200,  # Successful response
                success=True,
                data_quality_score=data_quality
            )
            
            return response
            
        except Exception as e:
            # Record failed call
            response_time = time.time() - start_time
            status_code = getattr(e, 'status_code', 0)
            
            self.monitor.record_api_call(
                api_name=self.api_name,
                response_time=response_time,
                status_code=status_code,
                success=False,
                error_message=str(e),
                data_quality_score=0.0
            )
            
            raise e
    
    def _assess_data_quality(self, data: Dict) -> float:
        """Assess the quality of returned data (0.0 to 1.0)"""
        if not data:
            return 0.0
        
        quality_score = 1.0
        
        # Check for required fields (basic implementation)
        if isinstance(data, dict):
            # Penalize for missing common fields
            expected_fields = self._get_expected_fields()
            missing_fields = len([f for f in expected_fields if f not in data])
            if expected_fields:
                quality_score -= (missing_fields / len(expected_fields)) * 0.3
        
        # Check for null values
        if isinstance(data, dict):
            null_values = len([v for v in data.values() if v is None])
            total_values = len(data.values())
            if total_values > 0:
                quality_score -= (null_values / total_values) * 0.2
        
        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, quality_score))
    
    def _get_expected_fields(self) -> List[str]:
        """Get list of expected fields for data quality assessment"""
        return []  # Override in subclasses
    
    # Override base methods to use monitoring
    def get(self, endpoint: str, params: Dict = None) -> Dict:
        return self._make_monitored_request('GET', endpoint, params=params)
    
    def post(self, endpoint: str, data: Dict = None, json_data: Dict = None) -> Dict:
        kwargs = {}
        if data:
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        return self._make_monitored_request('POST', endpoint, **kwargs)

class MonitoredDiseaseShClient(MonitoredAPIClient):
    """Disease.sh client with monitoring"""
    
    def __init__(self, monitor: APIHealthMonitor = None):
        super().__init__('https://disease.sh/v3', monitor=monitor)
        self.api_name = 'disease_sh'
    
    def _get_expected_fields(self) -> List[str]:
        return ['cases', 'deaths', 'recovered', 'active', 'country']
    
    def get_global_stats(self) -> Dict:
        """Get global COVID-19 statistics with monitoring"""
        try:
            data = self.get('/covid-19/all')
            self.logger.info("✅ Retrieved global COVID-19 statistics")
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'disease.sh',
                'data': data
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to get global stats: {str(e)}")
            raise
    
    def get_countries_data(self, countries: List[str] = None) -> List[Dict]:
        """Get COVID-19 data for countries with monitoring"""
        try:
            if countries:
                endpoint = f"/covid-19/countries/{','.join(countries)}"
            else:
                endpoint = "/covid-19/countries"
                
            data = self.get(endpoint)
            
            if not isinstance(data, list):
                data = [data]
                
            self.logger.info(f"✅ Retrieved data for {len(data)} countries")
            return [
                {
                    'timestamp': datetime.now().isoformat(),
                    'source': 'disease.sh',
                    'country': item['country'],
                    'data': item
                }
                for item in data
            ]
        except Exception as e:
            self.logger.error(f"❌ Failed to get countries data: {str(e)}")
            raise

class MonitoredRestCountriesClient(MonitoredAPIClient):
    """REST Countries client with monitoring"""
    
    def __init__(self, monitor: APIHealthMonitor = None):
        super().__init__('https://restcountries.com/v3.1', monitor=monitor)
        self.api_name = 'rest_countries'
    
    def _get_expected_fields(self) -> List[str]:
        return ['name', 'population', 'region', 'capital']
    
    def get_country_info(self, country: str) -> Dict:
        """Get country information with monitoring"""
        try:
            data = self.get(f"/name/{country}")
            if isinstance(data, list) and len(data) > 0:
                country_data = data[0]
            else:
                country_data = data
                
            self.logger.info(f"✅ Retrieved information for {country}")
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'restcountries.com',
                'country': country,
                'data': country_data
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to get country info for {country}: {str(e)}")
            raise

class MonitoredWorldBankClient(MonitoredAPIClient):
    """World Bank client with monitoring"""
    
    def __init__(self, monitor: APIHealthMonitor = None):
        super().__init__('https://api.worldbank.org/v2', monitor=monitor)
        self.api_name = 'world_bank'
    
    def _get_expected_fields(self) -> List[str]:
        return ['value', 'date', 'country']
    
    def get_gdp_data(self, country_code: str = 'WLD', years: int = 3) -> Dict:
        """Get GDP data with monitoring"""
        try:
            current_year = datetime.now().year
            start_year = current_year - years
            date_range = f"{start_year}:{current_year}"
            
            endpoint = f"/country/{country_code}/indicator/NY.GDP.MKTP.CD"
            params = {
                'date': date_range,
                'format': 'json',
                'per_page': 50
            }
            
            data = self.get(endpoint, params=params)
            
            if isinstance(data, list) and len(data) > 1:
                actual_data = data[1]
            else:
                actual_data = data
                
            self.logger.info(f"✅ Retrieved GDP data for {country_code}")
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'worldbank.org',
                'country_code': country_code,
                'indicator': 'GDP',
                'data': actual_data
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to get GDP data for {country_code}: {str(e)}")
            raise

class ResilientCOVIDIntegrator:
    """
    COVID data integrator with comprehensive monitoring and resilience
    """
    
    def __init__(self):
        # Shared monitor across all clients
        self.monitor = APIHealthMonitor("covid_integration_monitoring.db")
        
        # Initialize monitored clients
        self.disease_client = MonitoredDiseaseShClient(self.monitor)
        self.countries_client = MonitoredRestCountriesClient(self.monitor)
        self.worldbank_client = MonitoredWorldBankClient(self.monitor)
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_comprehensive_country_data(self, country: str) -> Dict:
        """Get comprehensive data with fallback strategies"""
        self.logger.info(f"🔄 Starting resilient data integration for {country}")
        
        result = {
            'country': country,
            'timestamp': datetime.now().isoformat(),
            'sources_attempted': [],
            'sources_successful': [],
            'fallbacks_used': []
        }
        
        # Try COVID data with fallback
        covid_data = self._get_covid_data_with_fallback(country)
        if covid_data:
            result['covid_stats'] = covid_data
            result['sources_successful'].append('disease.sh')
        
        # Try demographics with fallback
        demographics = self._get_demographics_with_fallback(country)
        if demographics:
            result['demographics'] = demographics
            result['sources_successful'].append('restcountries.com')
        
        # Try economic data with fallback
        economic_data = self._get_economic_data_with_fallback(country)
        if economic_data:
            result['economic_data'] = economic_data
            result['sources_successful'].append('worldbank.org')
        
        self.logger.info(f"✅ Integration complete: {len(result['sources_successful'])}/{len(result['sources_attempted'])} sources successful")
        return result
    
    def _get_covid_data_with_fallback(self, country: str) -> Optional[Dict]:
        """Get COVID data with fallback strategies"""
        self.logger.info(f"🦠 Getting COVID data for {country}")
        
        try:
            # Primary: Get specific country data
            covid_data = self.disease_client.get_countries_data([country])
            if covid_data:
                return covid_data[0]
        except Exception as e:
            self.logger.warning(f"⚠️ Primary COVID data failed: {str(e)}")
            
            try:
                # Fallback: Get global data as context
                self.logger.info("🔄 Falling back to global COVID data")
                global_data = self.disease_client.get_global_stats()
                global_data['fallback_reason'] = 'country_specific_unavailable'
                return global_data
            except Exception as e2:
                self.logger.error(f"❌ All COVID data sources failed: {str(e2)}")
                return None
    
    def _get_demographics_with_fallback(self, country: str) -> Optional[Dict]:
        """Get demographics with fallback"""
        self.logger.info(f"🌍 Getting demographics for {country}")
        
        try:
            # Primary: Get specific country info
            demographics = self.countries_client.get_country_info(country)
            return demographics
        except Exception as e:
            self.logger.warning(f"⚠️ Demographics failed: {str(e)}")
            
            try:
                # Fallback: Try alternative country names
                country_alternatives = {
                    'USA': ['United States', 'US', 'America'],
                    'UK': ['United Kingdom', 'Britain', 'England'],
                    'UAE': ['United Arab Emirates']
                }
                
                alternatives = country_alternatives.get(country.upper(), [])
                for alt_name in alternatives:
                    try:
                        self.logger.info(f"🔄 Trying alternative name: {alt_name}")
                        demographics = self.countries_client.get_country_info(alt_name)
                        demographics['fallback_reason'] = f'used_alternative_name_{alt_name}'
                        return demographics
                    except Exception:
                        continue
                        
                return None
            except Exception as e2:
                self.logger.error(f"❌ All demographics sources failed: {str(e2)}")
                return None
    
    def _get_economic_data_with_fallback(self, country: str) -> Optional[Dict]:
        """Get economic data with fallback"""
        self.logger.info(f"💰 Getting economic data for {country}")
        
        # Country code mapping
        country_codes = {
            'USA': 'US', 'United States': 'US',
            'India': 'IN', 'Brazil': 'BR',
            'Germany': 'DE', 'France': 'FR',
            'UK': 'GB', 'United Kingdom': 'GB',
            'China': 'CN', 'Japan': 'JP',
            'Italy': 'IT', 'Spain': 'ES',
            'Canada': 'CA', 'Australia': 'AU'
        }
        
        try:
            # Primary: Use mapped country code
            country_code = country_codes.get(country, country[:2].upper())
            economic_data = self.worldbank_client.get_gdp_data(country_code)
            return economic_data
        except Exception as e:
            self.logger.warning(f"⚠️ Economic data failed for {country}: {str(e)}")
            
            try:
                # Fallback: Get world data for context
                self.logger.info("🔄 Falling back to world economic data")
                world_data = self.worldbank_client.get_gdp_data('WLD')
                world_data['fallback_reason'] = 'country_specific_unavailable'
                return world_data
            except Exception as e2:
                self.logger.error(f"❌ All economic data sources failed: {str(e2)}")
                return None
    
    def run_health_check(self):
        """Run comprehensive health check on all APIs"""
        print("\n🔍 Running API Health Check...")
        print("=" * 50)
        
        # Check each API
        apis = ['disease_sh', 'rest_countries', 'world_bank']
        for api_name in apis:
            health = self.monitor.health_check(api_name)
            status_emoji = {
                'healthy': '✅',
                'degraded': '⚠️',
                'unhealthy': '❌',
                'unknown': '❓'
            }
            emoji = status_emoji.get(health.status.value, '❓')
            print(f"{emoji} {api_name}: {health.status.value.upper()}")
            print(f"   Success Rate: {health.success_rate*100:.1f}%")
            print(f"   Avg Response: {health.response_time:.2f}s")
            print(f"   Message: {health.message}")
        
        # Show circuit breaker states
        print("\n🔌 Circuit Breaker Status:")
        for api_name in apis:
            cb = self.monitor.get_circuit_breaker(api_name)
            state = cb.get_state()
            state_emoji = {'CLOSED': '🟢', 'OPEN': '🔴', 'HALF_OPEN': '🟡'}
            emoji = state_emoji.get(state['state'], '❓')
            print(f"{emoji} {api_name}: {state['state']}")
    
    def simulate_load_test(self, requests_per_api: int = 10):
        """Simulate load testing to generate monitoring data"""
        print(f"\n🧪 Simulating load test ({requests_per_api} requests per API)...")
        
        countries = ['USA', 'India', 'Brazil', 'Germany', 'France']
        
        for i in range(requests_per_api):
            country = countries[i % len(countries)]
            
            try:
                # Add some artificial delay to simulate real usage
                time.sleep(0.2)
                
                print(f"📡 Request {i+1}: Getting data for {country}")
                data = self.get_comprehensive_country_data(country)
                
                success_count = len(data['sources_successful'])
                total_sources = 3  # We have 3 API sources
                print(f"   ✅ Success: {success_count}/{total_sources} sources")
                
            except Exception as e:
                print(f"   ❌ Failed: {str(e)}")
        
        print("✅ Load test complete!")
    
    def generate_monitoring_report(self) -> Dict:
        """Generate comprehensive monitoring report"""
        dashboard_data = self.monitor.get_dashboard_data()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'report_type': 'api_monitoring_summary',
            'overall_health': dashboard_data['overall_health'],
            'api_summary': {},
            'recommendations': []
        }
        
        # Analyze each API
        for api_name, health_data in dashboard_data['api_health'].items():
            api_summary = {
                'status': health_data['status'],
                'success_rate': health_data['success_rate'],
                'response_time': health_data['response_time'],
                'performance_grade': 'A'  # Default
            }
            
            # Grade the API performance
            if health_data['success_rate'] < 0.9:
                api_summary['performance_grade'] = 'D'
                report['recommendations'].append(f"🚨 {api_name}: Low success rate requires immediate attention")
            elif health_data['success_rate'] < 0.95:
                api_summary['performance_grade'] = 'C'
                report['recommendations'].append(f"⚠️ {api_name}: Success rate below target, monitor closely")
            elif health_data['response_time'] > 3.0:
                api_summary['performance_grade'] = 'B'
                report['recommendations'].append(f"🐌 {api_name}: Response time is slow, consider optimization")
            
            report['api_summary'][api_name] = api_summary
        
        # General recommendations
        if dashboard_data['overall_health'] != 'HEALTHY':
            report['recommendations'].append("🔧 Consider implementing additional fallback mechanisms")
            report['recommendations'].append("📊 Increase monitoring frequency during degraded periods")
        
        return report

# Test the monitored and resilient system
if __name__ == "__main__":
    print("🧪 Testing Monitored and Resilient COVID API Integration")
    print("=" * 60)
    
    # Create the resilient integrator
    integrator = ResilientCOVIDIntegrator()
    
    # Test basic functionality
    print("\n📊 Testing basic integration...")
    try:
        usa_data = integrator.get_comprehensive_country_data('USA')
        print(f"✅ USA integration: {len(usa_data['sources_successful'])} sources successful")
        
        # Test with a potentially problematic country
        test_data = integrator.get_comprehensive_country_data('InvalidCountry')
        print(f"✅ Fallback test: {len(test_data['sources_successful'])} sources with fallbacks")
        
    except Exception as e:
        print(f"❌ Basic test failed: {str(e)}")
    
    # Run health check
    integrator.run_health_check()
    
    # Simulate some load to generate monitoring data
    integrator.simulate_load_test(5)
    
    # Run another health check to see the impact
    print("\n🔍 Health Check After Load Test:")
    integrator.run_health_check()
    
    # Generate monitoring report
    print("\n📊 Generating Monitoring Report...")
    report = integrator.generate_monitoring_report()
    
    print(f"📈 Overall System Health: {report['overall_health']}")
    print("\n📋 API Performance Grades:")
    for api_name, summary in report['api_summary'].items():
        print(f"   {api_name}: Grade {summary['performance_grade']} "
              f"({summary['success_rate']*100:.1f}% success, "
              f"{summary['response_time']:.2f}s avg)")
    
    if report['recommendations']:
        print("\n💡 Recommendations:")
        for rec in report['recommendations']:
            print(f"   {rec}")
    
    # Show final dashboard
    print("\n🖥️  Final Dashboard:")
    integrator.monitor.print_dashboard()
    
    print("\n✅ Monitored and resilient system testing complete!")
    print("\n🎯 Next: We'll create a real-time data pipeline and dashboard!")
