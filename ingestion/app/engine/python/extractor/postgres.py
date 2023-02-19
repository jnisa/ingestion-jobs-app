# Script that contains all the functions required to interact with our postgres database


def get_insert_query(table_id: str, record: dict, metadata: dict) -> str:
    """
    Defines the query that will be used to push a new record to an existing table.

    :param table_id: table that will receive the new record
    :param record: dictionary that contains the column names as well as the values 
    that will populated those columns.
    :param metadata: dictionary that reflects the metadata of the table under loading
    :return: a string with the query that will be used to insert new data on the 
    provided table
    """

    cols_lst = ", ".join(record.keys())
    vals_lst = ", ".join([set_values(v, metadata[k]) for k, v in record.items()])

    return f"INSERT INTO {table_id} ({cols_lst}) VALUES ({vals_lst})"


def get_create_query(table_id: str, metadata: dict, pk: str = None) -> str:
    """
    Defines the query that will be used to create a new table if that table that 
    will receive new data doesn't exist already.

    :param table_id: table that will be created
    :param metadata: table columns and their data types 
    :param pk: column that is ellected as the primary key
    :return: a string with the query that will be used to create a new table
    """

    cols_lst = list(metadata.keys())
    
    if pk == None:
        metadata[cols_lst[0]] += ' PRIMARY KEY'
    else:
        metadata[pk] += ' PRIMARY KEY'

    dtypes_lst = metadata.values()
    metadata_str = [f'{m[0]} {m[1]}'for m in list(zip(cols_lst, dtypes_lst))]

    return f"CREATE TABLE IF NOT EXISTS {table_id} ({', '.join(metadata_str)})"


def set_values(value, dtype: str) -> str:
    """
    Function that prepares the values to be included on the INSERT INTO query. E.g. if the
    the value of a certain column is a VARCHAR then the values must be encapsulated by '.

    :param value: record value that needs to be handled properly and converted in a string
    :param dtyoe: value data type
    :return: a string with the value that will be included on a certain query
    """

    # remove this from string values to avoid interferences with query generation
    str_cleansing = lambda x: x.replace("'", "")

    if (('BIGINT' in dtype) or ('NUMERIC' in dtype)) and not ('ARRAY' in dtype):
        return str(value)

    elif 'VARCHAR' in dtype or 'DATE' in dtype:
        return f"'{value}'"

    elif ('NUMERIC ARRAY' in dtype) or ('BIGINT ARRAY' in dtype):
        return "'{" + ', '.join([str(v) for v in value]) + "}'"

    elif 'text ARRAY' in dtype:
        return "'{" + f"""{', '.join([f'"{str_cleansing(v)}"' for v in value])}""" + "}'"
