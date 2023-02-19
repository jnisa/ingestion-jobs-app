# Script that tests all the methods developed to interact with the API

import requests
import unittest.mock


from ingestion.app.engine.python.extractor.api import get_jobs_request


class ApiIngestionTests(unittest.TestCase):
    
    def test_get_jobs_request_tc1(self):
        """
        get_jobs_request - Test Case Scenario 1
        
        Description: Check if the function correctly handles a successful response from the API,
        including the correct parsing of the JSON data.
        """
        
        with unittest.mock.patch('requests.get') as mock_get:
            mock_response = unittest.mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'SearchResult': {
                    'SearchResultItems': {
                        'key': 'value'
                    },
                    'UserArea': {
                        'NumberOfPages': 8
                    }
                }
            }
            mock_get.return_value = mock_response

            result_data, result_pages_number = get_jobs_request()
            expected = ({'key': 'value'}, 8)

            assert (result_data, result_pages_number) == expected
    

    def test_get_jobs_request_tc2(self):
        """
        get_jobs_requests - Test Case Scenario 2
        
        Description: Check if the function correctly handles an unsuccessful response from API,
        such as 4xx or 5xx status code, and raises and appropriate exception.
        """

        with unittest.mock.patch('requests.get') as mock_get:
            mock_response = unittest.mock.Mock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            with self.assertRaises(Exception) as context:
                get_jobs_request()
            
            self.assertEqual(str(context.exception), "Request failed with status code 404")

        
    def test_get_jobs_request_tc3(self):
        """
        get_jobs_request - Test Case Scenario 3

        Description: Check if the function correctly handles timeouts or network errors when making
        API requests.
        """

        with unittest.mock.patch('requests.get') as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout

            with self.assertRaises(Exception) as context:
                get_jobs_request()

            self.assertEqual(str(context.exception), "Request timed out")

