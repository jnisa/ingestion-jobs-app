# Script that contains the heart of the ingestion process

import pdb

from api import get_jobs_request
from auxiliars import preen, filter_dict
from ingestion.app.engine.python.extractor.postgres import (
    get_create_query,
    get_insert_query
)
from ingestion.app.engine.python.utils.configs import (
    # DB_CURSOR, 
    # DB_CONN, 
    DATASET_COLUMNS,
    TASMAN_ARRAY_COLUMNS,
    TASMAN_ARRAY_KEYS,
    TASMAN_METADATA
)


TABLE_NAME = 'USAJOBS_TASMAN'
PRIMARY_KEY = 'PositionID'


page_counter = 1

print("Let's start the ingestion!")

# TODO. remove this after testing
# DB_CURSOR.execute(f'DROP TABLE {TABLE_NAME};')

# create a postgres table to where the data will be loaded
# TODO. adapt this part with the next TODO content
# DB_CURSOR.execute(get_create_query(TABLE_NAME, TASMAN_METADATA, PRIMARY_KEY))


while True:

    data, pages_number = get_jobs_request(page_number = page_counter)

    pdb.set_trace()

    # perform some transformations on the data collected from the API
    for record in data:
        curated_record = preen(
            filter_dict(
                record['MatchedObjectDescriptor'],
                DATASET_COLUMNS,
                TASMAN_ARRAY_COLUMNS,
                TASMAN_ARRAY_KEYS
            )
        )

        # TODO. add a column with the ExtractionDate and ellect the column that joins 
        # PositionID and ExtractionDate (e.g. PositionID_dd_mm_year) as the PRIMARY KEY

        # push the record to table created initially
        DB_CURSOR.execute(get_insert_query(TABLE_NAME, curated_record, TASMAN_METADATA))


    # TODO. APPLY THIS AFTER TESTING -> if page_counter == int(pages_number):
    if page_counter == 1:
        DB_CONN.commit()
        DB_CURSOR.close()
        DB_CONN.close()
        break
    else:
        page_counter += 1


# TODO. create a table just with TORONTO DATA
