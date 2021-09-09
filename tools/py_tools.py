

def get(input_data: dict, keys: list):

    data = input_data
    for key in keys:
        data = data.get(key, None)
        if not data: return data

    return data
