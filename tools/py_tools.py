

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
