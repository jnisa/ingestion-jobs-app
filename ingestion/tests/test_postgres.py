# Script that tests all the methods developed to interact with the postgres database


import unittest.mock


from ingestion.app.engine.python.extractor.postgres import (
    get_create_query, 
    get_insert_query, 
    set_values
)

class GetInsertQueryTests(unittest.TestCase):
    
    def test_get_insert_query_tc1(self):
        """
        get_insert_query - Test Case Scenario 1
        
        Description: Test if the function performs as expected when only one column and 
        one value of BIGINT type are provided.
        """

        table_id = 'table_tc1'
        record = {'column_1': 1}
        metadata = {'column_1': 'BIGINT'}

        result = get_insert_query(table_id, record, metadata)
        expected = "INSERT INTO table_tc1 (column_1) VALUES (1)"
        
        self.assertEqual(result, expected)


    def test_get_insert_query_tc2(self):
        """
        get_insert_query - Test Case Scenario 2
        
        Description: Test if the function performs as expected when only one column and  
        one value of VARCHAR type are provided.
        """

        table_id = 'table_tc2'
        record = {'column_1': 'abc'}
        metadata = {'column_1': 'VARCHAR'}

        result = get_insert_query(table_id, record, metadata)
        expected = "INSERT INTO table_tc2 (column_1) VALUES ('abc')"
        
        self.assertEqual(result, expected)


    def test_get_insert_query_tc3(self):
        """
        get_insert_query - Test Case Scenario 3
        
        Description: Test if the function performs as expected when multiple columns and values
        with different data types.
        """

        table_id = 'table_tc3'
        record = {'column_1': 1, 'column_2': 'abc', 'column_3': 123}
        metadata = {'column_1': 'BIGINT', 'column_2': 'VARCHAR', 'column_3': 'BIGINT'}

        result = get_insert_query(table_id, record, metadata)
        expected = "INSERT INTO table_tc3 (column_1, column_2, column_3) VALUES (1, 'abc', 123)"
        
        self.assertEqual(result, expected)


    def test_get_insert_query_tc4(self):
        """
        get_insert_query - Test Case Scenario 4
        
        Description: Test if the function performs as expected when a DATA_TYPE[] column dtype
        is provided.
        """

        table_id = 'table_tc4'
        record = {'column_1': [1, 2, 3]}
        metadata = {'column_1': 'BIGINT ARRAY'}

        result = get_insert_query(table_id, record, metadata)
        expected = "INSERT INTO table_tc4 (column_1) VALUES ('{1, 2, 3}')"
        
        self.assertEqual(result, expected)


    def test_get_insert_query_tc5(self):
        """
        get_insert_query - Test Case Scenario 5
        
        Description: Test if the function performs as expected when a different data types are 
        provided.
        """

        table_id = 'table_tc5'
        record = {'column_1': [1, 2, 3], 'column_2': 12345, 'column_3': 'abc'}
        metadata = {'column_1': 'BIGINT ARRAY', 'column_2': 'BIGINT', 'column_3': 'VARCHAR'}

        result = get_insert_query(table_id, record, metadata)
        expected = "INSERT INTO table_tc5 (column_1, column_2, column_3) VALUES ('{1, 2, 3}', 12345, 'abc')"
        
        self.assertEqual(result, expected)


    def test_get_insert_query_tc6(self):
        """
        get_insert_query - Test Case Scenario 6
        
        Description: Test if the function performs as expected when a different data types are 
        provided and the string data types contains a character that will raise issues on the 
        INSERT INTO query.
        """

        table_id = 'table_tc6'
        record = {'column_1': ["Hudson, Florida", "Land 0' Lakes, Florida"], 'column_2': 12345}
        metadata = {'column_1': 'text ARRAY', 'column_2': 'BIGINT'}

        result = get_insert_query(table_id, record, metadata)
        expected = """INSERT INTO table_tc6 (column_1, column_2) VALUES ('{"Hudson, Florida", "Land 0 Lakes, Florida"}', 12345)"""
        
        self.assertEqual(result, expected)


class GetCreateQueryTests(unittest.TestCase):
    
    def test_get_create_query_tc1(self):
        """
        get_create_query - Test Case Scenario 1
        
        Description: Test if a the function responds as expected whenenever a table with one column
        needs to be created.
        """

        table_id = 'table_tc1'
        metadata = {'column_1': 'BOOLEAN'}

        result = get_create_query(table_id, metadata)
        expected = "CREATE TABLE IF NOT EXISTS table_tc1 (column_1 BOOLEAN PRIMARY KEY)"
        
        self.assertEqual(result, expected)


    def test_get_create_query_tc2(self):
        """
        get_create_query - Test Case Scenario 2
        
        Description: Test if the function performs as expected when we try to create a table with multiple 
        columns with different data types.
        """

        table_id = 'table_tc2'
        record = {'column_1': 'BIGINT', 'column_2': 'BOOLEAN', 'column_3': 'DATE'}

        result = get_create_query(table_id, record)
        expected = "CREATE TABLE IF NOT EXISTS table_tc2 (column_1 BIGINT PRIMARY KEY, column_2 BOOLEAN, column_3 DATE)"
        
        self.assertEqual(result, expected)


    def test_get_create_query_tc3(self):
        """
        get_create_query - Test Case Scenario 3
        
        Description: Test if the function performs as expected when we try to create a table with multiple 
        columns with different data types and we ellect a specific PRIMARY KEY.
        """

        table_id = 'table_tc3'
        record = {'column_1': 'BOOLEAN', 'column_2': 'DATE', 'column_3': 'BIGINT'}
        primary_key_column = 'column_3'

        result = get_create_query(table_id, record, primary_key_column)
        expected = "CREATE TABLE IF NOT EXISTS table_tc3 (column_1 BOOLEAN, column_2 DATE, column_3 BIGINT PRIMARY KEY)"
        
        self.assertEqual(result, expected)



class SetValuesTests(unittest.TestCase):
    
    def test_set_values_tc1(self):
        """
        set_values - Test Case Scenario 1
        
        Description: test if the function can handle properly a VARCHAR datatype value.
        """

        input_value = 'abc'
        input_dtype = 'VARCHAR'

        result = set_values(input_value, input_dtype)
        expected = "'abc'"
        
        self.assertEqual(result, expected)


    def test_set_values_tc2(self):
        """
        set_values - Test Case Scenario 2
        
        Description: test if the function can handle properly a NUMERIC datatype value.
        """

        input_value = 1234.5
        input_dtype = 'NUMERIC'

        result = set_values(input_value, input_dtype)
        expected = "1234.5"
        
        self.assertEqual(result, expected)


    def test_set_values_tc3(self):
        """
        set_values - Test Case Scenario 3
        
        Description: test if the function can handle properly a BIGINT datatype value.
        """

        input_value = 12345
        input_dtype = 'BIGINT'

        result = set_values(input_value, input_dtype)
        expected = "12345"
        
        self.assertEqual(result, expected)


    def test_set_values_tc4(self):
        """
        set_values - Test Case Scenario 4
        
        Description: test if the function can handle properly a DATATYPE[] datatype value.
        """

        input_value = ['a', 'b', 'c', 'd']
        input_dtype = 'text ARRAY'

        result = set_values(input_value, input_dtype)
        expected = """'{"a", "b", "c", "d"}'"""
        
        self.assertEqual(result, expected)


    def test_set_values_tc5(self):
        """
        set_values - Test Case Scenario 5
        
        Description: test if the function can handle properly a DATATYPE[] datatype value
        when the provided values are not strings.
        """

        input_value = [-1.0, 2.2, -3.3, 4.4]
        input_dtype = 'NUMERIC ARRAY'

        result = set_values(input_value, input_dtype)
        expected = "'{-1.0, 2.2, -3.3, 4.4}'"
        
        self.assertEqual(result, expected)
