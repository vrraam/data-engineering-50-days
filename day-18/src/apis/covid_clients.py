import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Now we can import our modules
try:
    from src.apis.base_client import BaseAPIClient, APIError
    from config.config import APIConfig
except ImportError as e:
    print(f"Import error: {e}")
    print("Let's try a different approach...")
    # Alternative import method
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_client_path = os.path.join(current_dir, 'base_client.py')
    config_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'config', 'config.py')
    
    # Import base_client directly
    import importlib.util
    spec = importlib.util.spec_from_file_location("base_client", base_client_path)
    base_client_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base_client_module)
    BaseAPIClient = base_client_module.BaseAPIClient
    APIError = base_client_module.APIError
    
    # Import config directly
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    APIConfig = config_module.APIConfig

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class DiseaseShClient(BaseAPIClient):
    """
    Client for disease.sh COVID-19 API
    Free API with comprehensive COVID-19 data
    """
    
    def __init__(self):
        # Simple config since we're having import issues
        base_url = 'https://disease.sh/v3'
        super().__init__(base_url=base_url)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def get_global_stats(self) -> Dict:
        """Get global COVID-19 statistics"""
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
        """Get COVID-19 data for specific countries or all countries"""
        try:
            if countries:
                # Get data for specific countries
                endpoint = f"/covid-19/countries/{','.join(countries)}"
            else:
                # Get data for all countries
                endpoint = "/covid-19/countries"
                
            data = self.get(endpoint)
            
            # Ensure we have a list
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
    
    def get_historical_data(self, country: str = 'all', days: int = 30) -> Dict:
        """Get historical COVID-19 data"""
        try:
            if country == 'all':
                endpoint = f"/covid-19/historical/all?lastdays={days}"
            else:
                endpoint = f"/covid-19/historical/{country}?lastdays={days}"
                
            data = self.get(endpoint)
            self.logger.info(f"✅ Retrieved {days} days of historical data for {country}")
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'disease.sh',
                'country': country,
                'days': days,
                'data': data
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to get historical data: {str(e)}")
            raise

class RestCountriesClient(BaseAPIClient):
    """
    Client for REST Countries API
    Free API for country information and demographics
    """
    
    def __init__(self):
        base_url = 'https://restcountries.com/v3.1'
        super().__init__(base_url=base_url)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_country_info(self, country: str) -> Dict:
        """Get detailed country information"""
        try:
            data = self.get(f"/name/{country}")
            if isinstance(data, list) and len(data) > 0:
                country_data = data[0]  # Take the first match
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

class WorldBankClient(BaseAPIClient):
    """
    Client for World Bank API
    Free API for economic indicators
    """
    
    def __init__(self):
        base_url = 'https://api.worldbank.org/v2'
        super().__init__(base_url=base_url)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_gdp_data(self, country_code: str = 'WLD', years: int = 3) -> Dict:
        """Get GDP data for a country"""
        try:
            # World Bank API format
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
            
            # World Bank API returns [metadata, data]
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

class COVIDDataIntegrator:
    """
    Main class that coordinates data from multiple APIs
    """
    
    def __init__(self):
        self.disease_client = DiseaseShClient()
        self.countries_client = RestCountriesClient()
        self.worldbank_client = WorldBankClient()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_comprehensive_country_data(self, country: str) -> Dict:
        """Get comprehensive data for a country from all sources"""
        self.logger.info(f"🔄 Gathering comprehensive data for {country}")
        
        result = {
            'country': country,
            'timestamp': datetime.now().isoformat(),
            'sources': []
        }
        
        try:
            # Get COVID data
            covid_data = self.disease_client.get_countries_data([country])
            if covid_data:
                result['covid_stats'] = covid_data[0]
                result['sources'].append('disease.sh')
                print(f"✅ COVID data: {covid_data[0]['data'].get('cases', 'N/A'):,} cases")
        except Exception as e:
            print(f"⚠️ Could not get COVID data: {str(e)}")
        
        try:
            # Get country demographics
            country_info = self.countries_client.get_country_info(country)
            result['demographics'] = country_info
            result['sources'].append('restcountries.com')
            population = country_info['data'].get('population', 'N/A')
            if isinstance(population, int):
                print(f"✅ Demographics: {population:,} population")
            else:
                print(f"✅ Demographics: {population} population")
        except Exception as e:
            print(f"⚠️ Could not get country info: {str(e)}")
        
        try:
            # Get economic data
            country_codes = {
                'usa': 'US', 'united states': 'US',
                'india': 'IN', 'brazil': 'BR',
                'germany': 'DE', 'france': 'FR',
                'uk': 'GB', 'united kingdom': 'GB'
            }
            
            country_code = country_codes.get(country.lower(), 'US')  # Default to US
            economic_data = self.worldbank_client.get_gdp_data(country_code)
            result['economic_data'] = economic_data
            result['sources'].append('worldbank.org')
            print(f"✅ Economic data: {len(economic_data['data']) if economic_data.get('data') else 0} data points")
        except Exception as e:
            print(f"⚠️ Could not get economic data: {str(e)}")
        
        self.logger.info(f"✅ Completed data integration for {country} using {len(result['sources'])} sources")
        return result

# Test the COVID API clients
if __name__ == "__main__":
    print("🧪 Testing COVID-19 API Clients")
    print("=" * 40)
    
    try:
        # Test Disease.sh client
        print("\n📊 Testing Disease.sh COVID Data...")
        disease_client = DiseaseShClient()
        
        # Get global stats
        global_stats = disease_client.get_global_stats()
        cases = global_stats['data'].get('cases', 'N/A')
        deaths = global_stats['data'].get('deaths', 'N/A')
        print(f"✅ Global COVID cases: {cases:,}" if isinstance(cases, int) else f"✅ Global COVID cases: {cases}")
        print(f"✅ Global COVID deaths: {deaths:,}" if isinstance(deaths, int) else f"✅ Global COVID deaths: {deaths}")
        
        # Test Countries client
        print("\n🌍 Testing REST Countries API...")
        countries_client = RestCountriesClient()
        
        usa_info = countries_client.get_country_info('USA')
        population = usa_info['data'].get('population', 'N/A')
        print(f"✅ USA population: {population:,}" if isinstance(population, int) else f"✅ USA population: {population}")
        
        # Test World Bank client
        print("\n💰 Testing World Bank API...")
        wb_client = WorldBankClient()
        
        us_gdp = wb_client.get_gdp_data('US')
        data_points = len(us_gdp['data']) if us_gdp.get('data') else 0
        print(f"✅ Retrieved US GDP data: {data_points} years of data")
        
        # Test integrated data
        print("\n🔗 Testing Data Integration...")
        integrator = COVIDDataIntegrator()
        
        comprehensive_data = integrator.get_comprehensive_country_data('USA')
        print(f"🎯 Final result: Integrated data from {len(comprehensive_data['sources'])} sources")
        print(f"   Sources used: {', '.join(comprehensive_data['sources'])}")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ COVID-19 API client testing complete!")
    print("\n🎯 Next: We'll add error handling and resilience patterns!")
