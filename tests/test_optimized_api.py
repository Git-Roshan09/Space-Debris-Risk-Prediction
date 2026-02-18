#!/usr/bin/env python3
"""
Test Script for Optimized TLE API
Verifies the new comprehensive dataset integration and API functionality
"""

import requests
import json
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TLEAPITester:
    """Test suite for the optimized TLE API"""
    
    def __init__(self, api_base_url="http://localhost:5000"):
        self.api_base_url = api_base_url
        self.test_results = []
    
    def log_test_result(self, test_name, success, details=""):
        """Log test result"""
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} - {test_name}")
        if details:
            logger.info(f"   Details: {details}")
        
        self.test_results.append({
            'test_name': test_name,
            'success': success,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
    
    def test_health_endpoint(self):
        """Test API health check"""
        try:
            response = requests.get(f"{self.api_base_url}/api/health", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                expected_keys = ['status', 'service', 'timestamp', 'cache_status']
                
                if all(key in data for key in expected_keys):
                    self.log_test_result("Health Endpoint", True, 
                                        f"Status: {data['status']}, Service: {data['service']}")
                    return True
                else:
                    self.log_test_result("Health Endpoint", False, "Missing expected keys in response")
                    return False
            else:
                self.log_test_result("Health Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Health Endpoint", False, f"Request failed: {e}")
            return False
    
    def test_stats_endpoint(self):
        """Test dataset statistics endpoint"""
        try:
            response = requests.get(f"{self.api_base_url}/api/stats", timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'success' in data and data['success'] and 'data' in data:
                    stats = data['data']
                    satellites = stats.get('satellites_count', 0)
                    debris = stats.get('debris_count', 0)
                    total = stats.get('total_objects', 0)
                    
                    self.log_test_result("Statistics Endpoint", True, 
                                        f"Total: {total:,}, Satellites: {satellites:,}, Debris: {debris:,}")
                    return True
                else:
                    self.log_test_result("Statistics Endpoint", False, "Invalid response format")
                    return False
            else:
                self.log_test_result("Statistics Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Statistics Endpoint", False, f"Request failed: {e}")
            return False
    
    def test_satellites_endpoint(self):
        """Test satellites data endpoint"""
        try:
            # Test basic satellite request
            params = {'limit': 10}
            response = requests.get(f"{self.api_base_url}/api/objects/satellites", 
                                   params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if ('success' in data and data['success'] and 
                    'data' in data and 'objects' in data['data']):
                    
                    objects = data['data']['objects']
                    classification = data['data']['classification']
                    
                    if len(objects) > 0 and classification == 'SATELLITE':
                        # Check object structure
                        sample_obj = objects[0]
                        required_fields = ['norad_id', 'name', 'tle_line1', 'tle_line2', 'classification']
                        
                        if all(field in sample_obj for field in required_fields):
                            self.log_test_result("Satellites Endpoint", True, 
                                               f"Retrieved {len(objects)} satellites, classification: {classification}")
                            return True
                        else:
                            missing_fields = [f for f in required_fields if f not in sample_obj]
                            self.log_test_result("Satellites Endpoint", False, 
                                               f"Missing fields: {missing_fields}")
                            return False
                    else:
                        self.log_test_result("Satellites Endpoint", False, 
                                           f"No objects or wrong classification: {classification}")
                        return False
                else:
                    self.log_test_result("Satellites Endpoint", False, "Invalid response structure")
                    return False
            else:  
                self.log_test_result("Satellites Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Satellites Endpoint", False, f"Request failed: {e}")
            return False
    
    def test_debris_endpoint(self):
        """Test debris data endpoint"""
        try:
            # Test basic debris request
            params = {'limit': 10}
            response = requests.get(f"{self.api_base_url}/api/objects/debris", 
                                   params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if ('success' in data and data['success'] and 
                    'data' in data and 'objects' in data['data']):
                    
                    objects = data['data']['objects']
                    classification = data['data']['classification']
                    
                    if len(objects) > 0 and classification == 'DEBRIS':
                        self.log_test_result("Debris Endpoint", True, 
                                           f"Retrieved {len(objects)} debris objects, classification: {classification}")
                        return True
                    else:
                        self.log_test_result("Debris Endpoint", False, 
                                           f"No objects or wrong classification: {classification}")
                        return False
                else:
                    self.log_test_result("Debris Endpoint", False, "Invalid response structure")
                    return False
            else:
                self.log_test_result("Debris Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Debris Endpoint", False, f"Request failed: {e}")
            return False
    
    def test_collision_pairs_endpoint(self):
        """Test collision pairs endpoint (SAT-SAT and SAT-DEB only)"""
        try:
            params = {'satellite_limit': 5, 'debris_limit': 5}
            response = requests.get(f"{self.api_base_url}/api/objects/collision-pairs", 
                                   params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if ('success' in data and data['success'] and 'data' in data):
                    collision_data = data['data']
                    satellites = collision_data.get('satellites', [])
                    debris = collision_data.get('debris', [])
                    collision_types = collision_data.get('collision_types_supported', [])
                    
                    expected_types = ['SAT-SAT', 'SAT-DEB']
                    if (len(satellites) > 0 and len(debris) > 0 and 
                        all(ct in collision_types for ct in expected_types)):
                        
                        self.log_test_result("Collision Pairs Endpoint", True, 
                                           f"Satellites: {len(satellites)}, Debris: {len(debris)}, Types: {collision_types}")
                        return True
                    else:
                        self.log_test_result("Collision Pairs Endpoint", False, 
                                           f"Insufficient data or wrong types: {collision_types}")
                        return False
                else:
                    self.log_test_result("Collision Pairs Endpoint", False, "Invalid response structure")
                    return False
            else:
                self.log_test_result("Collision Pairs Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Collision Pairs Endpoint", False, f"Request failed: {e}")
            return False
    
    def test_streaming_endpoint(self):
        """Test streaming endpoint (limited test)"""
        try:
            params = {'type': 'satellites', 'batch_size': 5, 'delay_ms': 100}
            response = requests.get(f"{self.api_base_url}/api/objects/stream", 
                                   params=params, timeout=10, stream=True)
            
            if response.status_code == 200:
                # Read first few chunks to verify streaming
                chunks_received = 0
                for chunk in response.iter_lines():
                    if chunk:
                        chunks_received += 1
                        if chunks_received >= 2:  # Just verify we get some data
                            break
                
                if chunks_received > 0:
                    self.log_test_result("Streaming Endpoint", True, 
                                       f"Received {chunks_received} data chunks")
                    return True
                else:
                    self.log_test_result("Streaming Endpoint", False, "No data chunks received")
                    return False
            else:
                self.log_test_result("Streaming Endpoint", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test_result("Streaming Endpoint", False, f"Request failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run complete test suite"""
        logger.info("=" * 60)
        logger.info("🧪 Starting TLE API Test Suite")
        logger.info("=" * 60)
        
        tests = [
            ("Health Check", self.test_health_endpoint),
            ("Dataset Statistics", self.test_stats_endpoint),
            ("Satellites Data", self.test_satellites_endpoint),
            ("Debris Data", self.test_debris_endpoint),
            ("Collision Pairs", self.test_collision_pairs_endpoint),
            ("Streaming Data", self.test_streaming_endpoint),
        ]
        
        passed_tests = 0
        total_tests = len(tests)
        
        for test_name, test_func in tests:
            logger.info(f"\n🔬 Running: {test_name}")
            try:
                if test_func():
                    passed_tests += 1
            except Exception as e:
                self.log_test_result(test_name, False, f"Test execution error: {e}")
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Tests Passed: {passed_tests}/{total_tests}")
        logger.info(f"Success Rate: {passed_tests/total_tests*100:.1f}%")
        
        if passed_tests == total_tests:
            logger.info("🎉 All tests passed! API is ready for production.")
        else:
            logger.info("⚠️  Some tests failed. Please check the API implementation.")
        
        return passed_tests == total_tests


def main():
    """Main test execution"""
    print("🚀 TLE API Integration Test")
    print("Testing optimized API with comprehensive dataset...")
    
    # Check if API is running
    tester = TLEAPITester()
    
    # Wait for API to be available
    logger.info("🔍 Checking API availability...")
    for attempt in range(3):
        try:
            response = requests.get(f"{tester.api_base_url}/api/health", timeout=5)
            if response.status_code == 200:
                logger.info("✅ API is running and accessible")
                break
        except:
            if attempt < 2:
                logger.info(f"⏳ API not ready, retrying in 5 seconds... (attempt {attempt + 1}/3)")
                time.sleep(5)
            else:
                logger.error("❌ API is not accessible. Please start the API first:")
                logger.error("   python optimized_tle_api.py")
                return False
    
    # Run tests
    return tester.run_all_tests()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)