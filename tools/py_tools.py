import time
import numpy as np


def delete_elements_from_array(array, drop_elements):
    drop_indicies = np.where(np.isin(array, drop_elements))
    array = np.delete(array, drop_indicies, 0)
    return array


def drop_columns_from_matrix(matrix, reference_array, col_names):
    if reference_array.ndim != 1:
        raise ValueError('keep_columns_from_matrix: Reference array must be a 1-D array')

    drop_cols = np.where(np.isin(reference_array, col_names))
    matrix = np.delete(matrix, drop_cols, 1)
    return matrix


def keep_columns_from_matrix(matrix, reference_array, col_names):
    if reference_array.ndim != 1:
        raise ValueError('keep_columsn_from_matrix: Reference array must be a 1-D array')

    keep_cols = np.where(np.isin(reference_array, col_names))[0]
    matrix = matrix[:, keep_cols]
    return matrix


def get(input_data: dict, keys: list, default=None):

    if input_data is None: return default

    data = input_data
    for key in keys:
        data = data.get(key, default)

        if data is None: break

    return data


def put(input_dict: dict, value, key_chain: list):

    key = key_chain.pop(0)

    if len(key_chain) == 0:
        input_dict[key] = value
    else:
        if not input_dict.get(key, None):
            input_dict[key] = dict()
        put(input_dict[key], value, key_chain)

    return input_dict


def expand_0D_np_array(array):

    if len(array.shape) > 1: return array
    return np.expand_dims(array, axis=1)



def timeit(function, return_output=False):

    start = time.time()
    out = function()
    end = time.time()
    time_elapsed = end - start

    if return_output:
        return dict(time_elapsed=time_elapsed, output=out)

    return dict(time_elapsed=time_elapsed, output=None)


def get_dtype_instance(dtype: str, fallback=None):

    if dtype == 'str': return str

    if dtype == 'int': return int

    if dtype == 'float': return float

    if dtype == 'bool': return bool

    if fallback: return fallback

    raise TypeError(f'Provided dtype of {dtype} is not a valid python type')


def convert_dtype(value, dtype: str):

    if dtype == 'str': return str(value)

    if dtype == 'int': return int(value)

    if dtype == 'float': return float(value)

    if dtype == 'bool': return bool(value)


def has_attr(object_inst: object, attribute: str):

    has = hasattr(object_inst, attribute)




