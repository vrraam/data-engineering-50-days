import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class APIConfig:
    """Simple configuration for our API integrations"""
    
    # Free APIs we'll use (no API key needed)
    FREE_APIS = {
        'disease_sh': {
            'base_url': 'https://disease.sh/v3',
            'description': 'COVID-19 statistics - Free API'
        },
        'rest_countries': {
            'base_url': 'https://restcountries.com/v3.1',
            'description': 'Country information - Free API'
        },
        'world_bank': {
            'base_url': 'https://api.worldbank.org/v2',
            'description': 'Economic data - Free API'
        }
    }
    
    # APIs that need keys (optional for now)
    PREMIUM_APIS = {
        'news_api': {
            'base_url': 'https://newsapi.org/v2',
            'api_key_env': 'NEWS_API_KEY',
            'description': 'News data - Requires API key'
        },
        'openweather': {
            'base_url': 'https://api.openweathermap.org/data/2.5',
            'api_key_env': 'OPENWEATHER_API_KEY',
            'description': 'Weather data - Requires API key'
        }
    }
    
    @classmethod
    def get_free_api_config(cls, api_name):
        """Get configuration for free APIs"""
        return cls.FREE_APIS.get(api_name, {})
    
    @classmethod
    def get_api_key(cls, api_name):
        """Get API key from environment if available"""
        api_config = cls.PREMIUM_APIS.get(api_name, {})
        env_var = api_config.get('api_key_env')
        if env_var:
            return os.getenv(env_var)
        return None
    
    @classmethod
    def list_all_apis(cls):
        """List all available APIs"""
        print("🆓 Free APIs (no key needed):")
        for name, config in cls.FREE_APIS.items():
            print(f"   • {name}: {config['description']}")
        
        print("\n💰 Premium APIs (key needed):")
        for name, config in cls.PREMIUM_APIS.items():
            key_status = "✅ Configured" if cls.get_api_key(name) else "❌ Not configured"
            print(f"   • {name}: {config['description']} - {key_status}")

# Test the configuration
if __name__ == "__main__":
    print("🔧 Configuration Check")
    print("=" * 30)
    APIConfig.list_all_apis()
    print("\n✅ Configuration loaded successfully!")
