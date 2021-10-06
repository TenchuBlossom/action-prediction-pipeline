import time

def get(input_data: dict, keys: list):

    data = input_data
    for key in keys:
        data = data.get(key, None)

        if data is None: break

    return data


def put(input_dict: dict, value, key_chain: list):

    key = key_chain.pop(0)

    if len(key_chain) == 0:
        input_dict[key] = value
    else:
        put(input_dict[key], value, key_chain)

    return input_dict


def timeit(function, return_output=False):

    start = time.time()
    out = function()
    end = time.time()
    time_elapsed = end - start

    if return_output:
        return dict(time_elapsed=time_elapsed, output=out)

    return dict(time_elapsed=time_elapsed, output=None)
