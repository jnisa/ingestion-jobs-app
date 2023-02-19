# Script that contains the heart of the ingestion process

from datetime import datetime

from api import get_jobs_request
from auxiliars import preen, filter_dict
from ingestion.app.engine.python.extractor.postgres import (
    get_create_query,
    get_insert_query
)
from ingestion.app.engine.python.utils.configs import (
    DB_CURSOR, 
    DB_CONN, 
    DATASET_COLUMNS,
    TASMAN_ARRAY_COLUMNS,
    TASMAN_ARRAY_KEYS,
    TASMAN_METADATA
)


TABLE_NAME = 'USAJOBS_TASMAN'
PRIMARY_KEY = 'ID'
CURR_DATE = datetime.now()

page_counter = 1
date_value = f"""{"{:02d}".format(CURR_DATE.day)}-{"{:02d}".format(CURR_DATE.month)}-{CURR_DATE.year}"""

# create a postgres table to where the data will be loaded
DB_CURSOR.execute(get_create_query(TABLE_NAME, TASMAN_METADATA, PRIMARY_KEY))


while True:

    data, pages_number = get_jobs_request(page_number = page_counter)

    # perform some transformations on the data collected from the API
    for record in data:

        location = str(record['MatchedObjectDescriptor']['PositionLocation'])

        # requested filter
        if "Chicago, Illinois" in location:

            curated_record = preen(
                filter_dict(
                    record['MatchedObjectDescriptor'],
                    DATASET_COLUMNS,
                    TASMAN_ARRAY_COLUMNS,
                    TASMAN_ARRAY_KEYS
                )
            )

            primary_key = {'ID': '_'.join([curated_record['PositionID'], date_value])}
            curated_record = {**primary_key, **curated_record} 
            curated_record['ExtractionDate'] = date_value

            # push the record to table created initially
            DB_CURSOR.execute(get_insert_query(TABLE_NAME, curated_record, TASMAN_METADATA))


    if page_counter == int(pages_number):
        DB_CONN.commit()
        DB_CURSOR.close()
        DB_CONN.close()
        break
    else:
        page_counter += 1
