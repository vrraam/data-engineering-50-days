import requests
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from retrying import retry
import os
from urllib.parse import urljoin, urlencode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api_integration.log'),
        logging.StreamHandler()
    ]
)

class APIError(Exception):
    """Custom exception for API-related errors"""
    def __init__(self, message: str, status_code: int = None, response_data: Dict = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)

class RateLimitError(APIError):
    """Exception for rate limit errors"""
    pass

class AuthenticationError(APIError):
    """Exception for authentication errors"""
    pass

class BaseAPIClient:
    """
    Base API client with built-in resilience patterns:
    - Retry logic with exponential backoff
    - Rate limiting handling
    - Circuit breaker pattern
    - Request/response logging
    - Error classification and handling
    """
    
    def __init__(self, base_url: str, api_key: str = None, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Circuit breaker state
        self.failure_count = 0
        self.last_failure_time = None
        self.circuit_open = False
        self.failure_threshold = 5
        self.recovery_timeout = 300  # 5 minutes
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1  # seconds between requests
        
        # Setup session headers
        self._setup_session()
        
    def _setup_session(self):
        """Setup default session headers and configuration"""
        self.session.headers.update({
            'User-Agent': 'DataEngineering-Day18-Bot/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        if self.api_key:
            # Different APIs use different auth header formats
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}',
                'X-API-Key': self.api_key
            })
    
    def _check_circuit_breaker(self):
        """Check if circuit breaker allows requests"""
        if not self.circuit_open:
            return True
            
        # Check if we can try to recover
        if (datetime.now() - self.last_failure_time).seconds > self.recovery_timeout:
            self.logger.info("Circuit breaker: Attempting recovery")
            self.circuit_open = False
            self.failure_count = 0
            return True
            
        raise APIError("Circuit breaker is open - too many consecutive failures")
    
    def _handle_rate_limiting(self):
        """Implement basic rate limiting"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            self.logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """Handle and classify API responses"""
        
        # Log response details
        self.logger.info(f"API Response: {response.status_code} - {response.url}")
        
        # Success responses
        if 200 <= response.status_code < 300:
            self.failure_count = 0  # Reset failure count on success
            try:
                return response.json()
            except json.JSONDecodeError:
                self.logger.warning("Response is not valid JSON, returning text")
                return {"data": response.text}
        
        # Handle different error types
        error_data = None
        try:
            error_data = response.json()
        except json.JSONDecodeError:
            error_data = {"error": response.text}
        
        # Rate limiting (429 or specific rate limit messages)
        if response.status_code == 429 or "rate limit" in str(error_data).lower():
            # Extract retry-after header if available
            retry_after = response.headers.get('Retry-After', 60)
            try:
                retry_after = int(retry_after)
            except (ValueError, TypeError):
                retry_after = 60
                
            self.logger.warning(f"Rate limit hit, retry after {retry_after} seconds")
            raise RateLimitError(f"Rate limit exceeded, retry after {retry_after}s", 
                               response.status_code, error_data)
        
        # Authentication errors
        if response.status_code in [401, 403]:
            self.logger.error("Authentication failed")
            raise AuthenticationError("Authentication failed", 
                                    response.status_code, error_data)
        
        # Server errors (5xx) - these are retryable
        if 500 <= response.status_code < 600:
            self._record_failure()
            raise APIError(f"Server error: {response.status_code}", 
                          response.status_code, error_data)
        
        # Client errors (4xx) - these are generally not retryable
        if 400 <= response.status_code < 500:
            raise APIError(f"Client error: {response.status_code}", 
                          response.status_code, error_data)
        
        # Unknown error
        self._record_failure()
        raise APIError(f"Unknown error: {response.status_code}", 
                      response.status_code, error_data)
    
    def _record_failure(self):
        """Record API failure for circuit breaker"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            self.logger.error(f"Circuit breaker opened after {self.failure_count} failures")
    
    @retry(
        retry_on_exception=lambda x: isinstance(x, (APIError, RateLimitError)) and not isinstance(x, AuthenticationError),
        wait_exponential_multiplier=1000,  # Start with 1 second
        wait_exponential_max=30000,        # Max 30 seconds
        stop_max_attempt_number=3
    )
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with retry logic"""
        
        # Check circuit breaker
        self._check_circuit_breaker()
        
        # Handle rate limiting
        self._handle_rate_limiting()
        
        # Construct full URL
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        
        # Log request
        self.logger.info(f"Making {method.upper()} request to {url}")
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                timeout=self.timeout,
                **kwargs
            )
            
            return self._handle_response(response)
            
        except requests.exceptions.Timeout:
            self._record_failure()
            raise APIError("Request timeout")
        except requests.exceptions.ConnectionError:
            self._record_failure()
            raise APIError("Connection error")
        except requests.exceptions.RequestException as e:
            self._record_failure()
            raise APIError(f"Request failed: {str(e)}")
    
    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make GET request"""
        return self._make_request('GET', endpoint, params=params)
    
    def post(self, endpoint: str, data: Dict = None, json_data: Dict = None) -> Dict[str, Any]:
        """Make POST request"""
        kwargs = {}
        if data:
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        return self._make_request('POST', endpoint, **kwargs)
    
    def put(self, endpoint: str, data: Dict = None, json_data: Dict = None) -> Dict[str, Any]:
        """Make PUT request"""
        kwargs = {}
        if data:
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        return self._make_request('PUT', endpoint, **kwargs)
    
    def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request"""
        return self._make_request('DELETE', endpoint)
    
    def health_check(self) -> bool:
        """Check if the API is healthy"""
        try:
            # Most APIs have a health or status endpoint
            # This is a generic implementation
            response = self.get('/health')
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current client status"""
        return {
            'circuit_open': self.circuit_open,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'base_url': self.base_url,
            'authenticated': bool(self.api_key)
        }

# Example usage and testing
if __name__ == "__main__":
    # Test the base client with a public API
    print("🧪 Testing Base API Client...")
    
    # We'll use JSONPlaceholder for testing (no API key needed)
    client = BaseAPIClient("https://jsonplaceholder.typicode.com")
    
    try:
        # Test GET request
        print("\n📡 Testing GET request...")
        posts = client.get("/posts", params={"_limit": 5})
        print(f"✅ Retrieved {len(posts)} posts")
        
        # Test POST request
        print("\n📤 Testing POST request...")
        new_post = client.post("/posts", json_data={
            "title": "Test Post",
            "body": "This is a test post from our API client",
            "userId": 1
        })
        print(f"✅ Created post with ID: {new_post.get('id')}")
        
        # Test client status
        print("\n📊 Client Status:")
        status = client.get_status()
        for key, value in status.items():
            print(f"   {key}: {value}")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
    
    print("\n✅ Base API Client testing complete!")
