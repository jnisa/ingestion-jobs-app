# Script that contains auxiliar function that might be used by the pipeline

from collections.abc import MutableMapping


def preen(d: dict, parent_key = '', sep = '_') -> dict:
    """
    Whenever a python dictionary with nested fields needs some preening this function
    converts all second layer keys (or deeper hierarchies) or more will dragged to the
    root level.

    Example:
        Input:
        {
            'firstLevel': {
                'attribute_1': 2
            }
        }

        Output:
        {
            'firstLevel_attribute_1': 2
        }

    :param d: nested dictionary that is about to be unfolded
    :param parent_key: corresponds to the name of dimensions
    :param sep: separator that stays in between the dimension and the column name
    :return: python dictionary flatten with no nested dictionaries
    """

    items = []
    for k, v in d.items():
    
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, MutableMapping):
            items.extend(preen(v, new_key, sep = sep).items())

        else:
            items.append((new_key, v))
    
    return dict(items)


def filter_dict(d: dict, cols2keep: list, array_columns: list = [], array_keys: list = []) -> dict:
    """
    Remove all the keys that are not on the list of columns to keep. In addition
    to this, all key that have a dictionary encapsulated in a list as a value are
    also decapsulated. 

    :param d: provided python dictionary
    :param cols2keep: list of columns
    :param array_columns: columns that must be converted into arrays
    :param array_keys: subcolumns that will be used to filter the records
    :return: the same provided dictionary as an input but only with the keys that were
    on the list of columns provided
    """

    return {
        k: (
            convert_to_array(v, array_keys) 
            if k in array_columns 
            else (v[0] if isinstance(v, list) and len(v) == 1 else v)
        ) 
        for k, v in d.items() if k in cols2keep
    }


def convert_to_array(vals_lst: list, keys: list = []) -> dict:
    """
    When a key from a python dictionary has a list of dictionaries as a value 
    we use this function to keep everything in one dictionary only and convert the
    values of each key into lists.

    Example:
        Input:
            [
                {'subkey1': 1, 'subkey2': 2},
                {'subkey1': 3, 'subkey2': 4}
            ]

        Output:
            {
                'subkey1': [1, 3], 
                'subkey2': [2, 4]
            }
    
    :param vals_lst: list of dictionaries that a given key from a python dictionary possesses
    as a value
    :param keys: keys that must be kept from all the values provided in the previous list
    :return: a dictionary that has converted all the list of dictionaries into lists of 
    values only 
    """
    
    if keys == []:
        keys = list(vals_lst[0].keys())

    # TODO. ADD THIS STEP TO THE DOCUMENTATION - PositionLocation_AddressLine??
    vals_duplicated = [list(val.values()) for val in vals_lst if list(val.keys()) == keys]
    vals_deduplicated = list(map(list, zip(*vals_duplicated)))

    return dict(zip(keys, vals_deduplicated))
