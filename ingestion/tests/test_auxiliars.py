# Script that tests all the auxiliar functions 

import unittest


from ingestion.app.engine.python.extractor.auxiliars import preen, filter_dict, convert_to_array


class PreenTests(unittest.TestCase):

    def test_preen_tc1(self):
        """
        preen - Test Case Scenario 1

        Description: test if the function can handle a single nested dictionary, to verify
        if correctly handles the unfolding of a single nested level.
        """

        test_input = {
            'firstLevel': {
                'attribute_1': 2
            }
        }

        expected = {
            'firstLevel_attribute_1': 2
        }
        result = preen(test_input)

        self.assertEqual(result, expected)


    def test_preen_tc2(self):
        """
        preen - Test Case Scenario 2

        Description: test if the function with an input that contains multiple nested levels, 
        to verify that it correctly handles unfolding multiple nested levels.
        """

        test_input = {
            'firstLevel': {
                'attribute_1': 3,
                'attribute_2': {
                    'subLevel': 2
                }
            }
        }

        expected = {
            'firstLevel_attribute_1': 3,
            'firstLevel_attribute_2_subLevel': 2
        }
        result = preen(test_input)

        self.assertEqual(result, expected)


    def test_preen_tc3(self):
        """
        preen - Test Case Scenario 3

        Description: test if the function with an input that contains nested dictionaries with 
        different keys, to verify that if correctly handles the values of nested dictionaries. 
        """

        test_input = {
            'firstLevel': {
                'attribute_1': 5
            },
            'secondLevel': {
                'attribute_1': 1,
                'attribute_2': {
                    'subLevel': 2
                }
            }
        }

        expected = {
            'firstLevel_attribute_1': 5,
            'secondLevel_attribute_1': 1,
            'secondLevel_attribute_2_subLevel': 2
        }
        result = preen(test_input)

        self.assertEqual(result, expected)


    def test_preen_tc4(self):
        """
        preen - Test Case Scenario 4

        Description: test if the function with an input that contains nested dictionaries with 
        values with different data types to verify that if correctly handles the values of nested
        dictionaries. 
        """

        test_input = {
            'firstLevel': {
                'attribute_1': 'abc'
            },
            'secondLevel': {
                'attribute_1': [1, 2, 3, 4],
                'attribute_2': {
                    'subLevel': (1, 3, 5)
                }
            }
        }    

        expected = {
            'firstLevel_attribute_1': 'abc',
            'secondLevel_attribute_1': [1, 2, 3, 4],
            'secondLevel_attribute_2_subLevel': (1, 3, 5)
        }
        result = preen(test_input)

        self.assertEqual(result, expected)


    def test_preen_tc5(self):
        """
        preen - Test Case Scenario 5

        Description: test if the function with an input that contains nested dictionaries with 
        an empty input. 
        """

        test_input = {}

        expected = {}
        result = preen(test_input)

        self.assertEqual(result, expected)



class FilterDictTests(unittest.TestCase):
    

    def test_filter_dict_tc1(self):
        """
        filter_dict - Test Case Scenario 1

        Description: Check if the function filters correctly the only existent key.
        """

        input_dict = {
            'onlyKey': 1
        }
        input_cols2keep = []

        expected = {}
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)
    

    def test_filter_dict_tc2(self):
        """
        filter_dict - Test Case Scenario 2

        Description: Check if the function filters correctly a dictionary with multiple keys when all
        of them must be filtered.
        """

        input_dict = {
            'firstKey': 1,
            'secondKey': 2,
            'thirdKey': 3
        }
        input_cols2keep = []

        expected = {}
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)
    

    def test_filter_dict_tc3(self):
        """
        filter_dict - Test Case Scenario 3

        Description: Check if the function filters correctly a dictionary with multiple keys - some
        keys should be kept and other should be excluded from the obtained dictionary.
        """

        input_dict = {
            'firstKey': 1,
            'secondKey': 2,
            'thirdKey': 3,
            'forthKey': 4
        }
        input_cols2keep = ['firstKey', 'thirdKey']

        expected = {
            'firstKey': 1,
            'thirdKey': 3
        }
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)
    

    def test_filter_dict_tc4(self):
        """
        filter_dict - Test Case Scenario 4

        Description: Check if the function filters correctly a dictionary that has keys with 
        different data types.
        """

        input_dict = {
            1: 'firstValue',
            'abc': 'secondValue',
            (1, 2, 3): 'thirdValue'
        }
        input_cols2keep = ['abc']

        expected = {
            'abc': 'secondValue'
        }
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)
    

    def test_filter_dict_tc5(self):
        """
        filter_dict - Test Case Scenario 5

        Description: Check if the function behaves as expected when an empty dictionary is provided.
        """

        input_dict = {}
        input_cols2keep = ['firstValue', 'secondValue']

        expected = {}
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)


    def test_filter_dict_tc6(self):
        """
        filter_dict - Test Case Scenario 6

        Description: test if the function behaves as expected when an nested dictionary is 
        encapsulated by a list is in the input variable but it's not a column that needs to be
        converted to an array. 
        """

        input_dict = {
            'firstLevel': [
                {
                    'firstAttribute': 1,
                    'secondAttribute': 2
                }
            ]
        }
        input_cols2keep = ['firstLevel']


        expected = {
            'firstLevel':
                {
                    'firstAttribute': 1,
                    'secondAttribute': 2
                }        
        }
        result = filter_dict(input_dict, input_cols2keep)

        self.assertEqual(result, expected)


    def test_filter_dict_tc7(self):
        """
        filter_dict - Test Case Scenario 7

        Description: test if the function behaves as expected when an nested dictionary is 
        encapsulated by a list is in the input variable and it's a column that needs to be
        converted to an array. 
        """

        input_dict = {
            'firstLevel': [
                {
                    'firstAttribute': 1,
                    'secondAttribute': 2
                }
            ]
        }
        input_cols2keep = ['firstLevel']
        input_array_columns = ['firstLevel']


        expected = {
            'firstLevel':
                {
                    'firstAttribute': [1],
                    'secondAttribute': [2]
                }        
        }
        result = filter_dict(input_dict, input_cols2keep, input_array_columns)

        self.assertEqual(result, expected)


    def test_filter_dict_tc8(self):
        """
        filter_dict - Test Case Scenario 8

        Description: test if the function behaves as expected when an nested dictionary is 
        encapsulated by a list of dictionaries with the same structure is in the input variable. 
        """

        input_dict = {
            'firstLevel': [
                {'subkey_1': 1, 'subkey_2': 2},
                {'subkey_1': 3, 'subkey_2': 4},
                {'subkey_1': 5, 'subkey_2': 6}
            ]
        }
        input_cols2keep = ['firstLevel']
        input_array_columns = ['firstLevel']


        expected = {
            'firstLevel':
                {
                    'subkey_1': [1, 3, 5],
                    'subkey_2': [2, 4, 6]
                }        
        }
        result = filter_dict(input_dict, input_cols2keep, input_array_columns)

        self.assertEqual(result, expected)


    def test_filter_dict_tc9(self):
        """
        filter_dict - Test Case Scenario 9

        Description: test if the function behaves as expected when an nested dictionary is 
        encapsulated by a list of dictionaries with the same structure is in the input variable
        and only a set of subkeys is selected. 
        """

        input_dict = {
            'firstLevel': [
                {'subkey_1': 1, 'subkey_2': 2},
                {'subkey_1': 3},
                {'subkey_1': 5}
            ]
        }
        input_cols2keep = ['firstLevel']
        input_array_columns = ['firstLevel']
        input_array_keys = ['subkey_1']

        expected = {
            'firstLevel':
                {
                    'subkey_1': [3, 5]
                }        
        }
        result = filter_dict(input_dict, input_cols2keep, input_array_columns, input_array_keys)

        self.assertEqual(result, expected)



class ConvertToArrayTests(unittest.TestCase):
    
    def test_convert_to_array_tc1(self):
        """
        convert_to_array - Test Case Scenario 1

        Description: test if the function can handle a list with same dictionary keys and 
        different values to each key.
        """

        input_list = [
            {'subkey_1': 1, 'subkey_2': 2},
            {'subkey_1': 3, 'subkey_2': 4}
        ]

        expected = {
            'subkey_1': [1, 3], 
            'subkey_2': [2, 4]
        }
        result = convert_to_array(input_list)

        self.assertEqual(result, expected)


    def test_convert_to_array_tc2(self):
        """
        convert_to_array - Test Case Scenario 2

        Description: test if the function can handle a list with the same dictionary keys with
        one of the keys having the exact same value.
        """

        input_list = [
            {'subkey_1': 1, 'subkey_2': 2},
            {'subkey_1': 1, 'subkey_2': 4}
        ]

        expected = {
            'subkey_1': [1, 1], 
            'subkey_2': [2, 4]
        }
        result = convert_to_array(input_list)

        self.assertEqual(result, expected)


    def test_convert_to_array_tc3(self):
        """
        convert_to_array - Test Case Scenario 3

        Description: test if the function can handle a list with one value only on it.
        """

        input_list = [
            {'subkey_1': 1, 'subkey_2': 2}
        ]

        expected = {
            'subkey_1': [1], 
            'subkey_2': [2]
        }
        result = convert_to_array(input_list)

        self.assertEqual(result, expected)


    def test_convert_to_array_tc4(self):
        """
        convert_to_array - Test Case Scenario 4

        Description: test if the function can handle a list with one value that have keys that
        are not in compliance with the API documentation.
        """

        input_list = [
            {'subkey_1': 1, 'subkey_2': 3},
            {'subkey_1': 2, 'subkey_4': 4},
            {'subkey_1': 5, 'subkey_2': 6},
            {'subkey_1': 7, 'subkey_5': 8}
        ]
        input_keys = ['subkey_1', 'subkey_2']

        expected = {
            'subkey_1': [1, 5], 
            'subkey_2': [3, 6]
        }
        result = convert_to_array(input_list, input_keys)

        self.assertEqual(result, expected)