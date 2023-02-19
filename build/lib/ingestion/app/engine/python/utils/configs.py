# Script that contains the variables that will be used across the pipeline

import psycopg2

API_KEY = '9MbHa87/i38Y36f7BjnF/HnyPGEsOUWXJdInME0B99E='
BASE_URL = 'https://data.usajobs.gov/api/search?'

DATASET_COLUMNS = [
    'PositionID',
    'PositionTitle',
    'PositionURI',
    'PositionLocation',
    'OrganizationName',
    'DepartmentName',
    'PositionSchedule',
    'PositionOfferingType',
    'PositionRemuneration',
]

DTYPES_MAP = {
    'int': 'BIGINT',
    'bool': 'BOOLEAN',
    'bytes': 'BYTEA',
    'str': 'VARCHAR',
    'datetime': 'DATE',
    'float': 'NUMERIC'
}

TASMAN_METADATA = {
    'PositionID': 'VARCHAR', 
    'PositionTitle': 'VARCHAR', 
    'PositionURI': 'VARCHAR', 
    'PositionLocation_LocationName': 'text ARRAY', 
    'PositionLocation_CountryCode': 'text ARRAY', 
    'PositionLocation_CountrySubDivisionCode': 'text ARRAY', 
    'PositionLocation_CityName': 'text ARRAY', 
    'PositionLocation_Longitude': 'NUMERIC ARRAY', 
    'PositionLocation_Latitude': 'NUMERIC ARRAY', 
    'OrganizationName': 'VARCHAR', 
    'DepartmentName': 'VARCHAR', 
    'PositionSchedule_Name': 'VARCHAR', 
    'PositionSchedule_Code': 'BIGINT', 
    'PositionOfferingType_Name': 'VARCHAR', 
    'PositionOfferingType_Code': 'BIGINT', 
    'PositionRemuneration_MinimumRange': 'NUMERIC', 
    'PositionRemuneration_MaximumRange': 'NUMERIC', 
    'PositionRemuneration_RateIntervalCode': 'VARCHAR', 
    'PositionRemuneration_Description': 'VARCHAR'
}

TASMAN_ARRAY_COLUMNS = [
    'PositionLocation'
]

TASMAN_ARRAY_KEYS = [
    'LocationName',
    'CountryCode',
    'CountrySubDivisionCode',
    'CityName',
    'Longitude',
    'Latitude'
]

# DB_CONN = psycopg2.connect(
#     host="usajobs-db",
#     database="tasman_db",
#     user="tasman_user",
#     password="tasman_senior_data_engineer"
# )

# DB_CURSOR = DB_CONN.cursor()