import collections.abc
import random
import pandas as pd
import math
from copy import deepcopy


def preview_data(database: collections.abc.MutableMapping, preview_frac: float, random_seed: int):

    random.seed(random_seed)
    total_length = sum([val['length'] for _, val in database.items()])
    no_preview_samples = math.floor(total_length * preview_frac)
    for _, val in database.items():
        samples = math.floor(val['ratio'] * no_preview_samples)
        sample_index = random.sample(range(val['length']), samples)

        val['index'] = [val['index'][i] for i in sample_index]
        val['partition'] = [val['partition'][i] for i in sample_index]
        val['byte_size'] = [val['byte_size'][i] for i in sample_index]
        val['strat_sample_length'] = samples
        val['length'] = samples

    return database


def to_virtual_dataframe(database: collections.abc.MutableMapping, name=None, preview_frac=1, random_seed=None, copy=True):
    database = deepcopy(database) if copy else database
    database = preview_data(database, preview_frac, random_seed)
    rows = []
    for val_name, val_content in database.items():
        index = val_content['index']
        partition = val_content['partition']
        for i, p in zip(index, partition):
            value = val_name
            rows.append([i, p, value])

    df = pd.DataFrame(rows, columns=['index', 'partition', name])
    return df


