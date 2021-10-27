import collections.abc
import random
import pandas as pd
import json
import os
import tools.file_system as fs
from mergedeep import merge, Strategy
from tqdm import tqdm
from alive_progress import alive_bar
import math
import tools.py_tools as pyt
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


class Paths:
    def __init__(self, root, row, headers, database):
        self.root = root
        self.row = row
        self.headers = headers
        self.database = database


class Partition:
    def __init__(self, paths, name=None):
        self.name = name
        self.paths = Paths(**paths)
        self.database = fs.load_json(self.paths.database)
        self.headers = pd.read_csv(self.paths.headers).values[:, 0]


class VirtualDb:

    def __init__(self, pathname: str):
        self.pathname = pathname
        self.partitions = dict()

    def anchor(self):
        dir_check = lambda file: file.endswith('partition')
        for partition_name, partition_path in zip(*fs.get_dirs(self.pathname, custom_check=dir_check)):
            row_path = fs.get_dirs(partition_path, custom_check=lambda file: file == 'rows')[1][0]
            header_path, virtual_db_path = fs.find_files(partition_path, ['headers.csv', 'virtual_db.json'])[1]

            paths = dict(root=partition_path, row=row_path, headers=header_path, database=virtual_db_path)
            self.partitions[partition_name] = Partition(paths, partition_name)

    def view(self, partitions=None, view_all=False, merge_partitions=False):
        if partitions is None: partitions = []
        if view_all or len(partitions) == 0:
            partitions = self.partitions.keys()

        out_db = [self.partitions[name].database for name in partitions]

        if merge_partitions:
            out_db = merge({}, *out_db, strategy=Strategy.ADDITIVE)

        # Calculate the ratios of each unique value in db fields
        for field, content in out_db.items():
            ratios = [len(value['index']) for _, value in content.items()]
            r_sum = sum(ratios)

            for length, (val_name, value) in zip(ratios, content.items()):
                value['length'] = length
                value['ratio'] = round(length / r_sum, 2)

            round_check = sum([value['ratio'] for _, value in content.items()])
            if round_check != 1.0:
                raise ValueError('Parition Rounding Error: stratifying ratios of a partition column summed to value'
                                 'not equal to 1.0')

        return out_db

    def compute(self, virtual_dataframe: pd.DataFrame, dtype, middleware=None):

        if virtual_dataframe.shape[1] != 2:
            raise ValueError(f'Error Virtual Dataframe Shape: received shape {virtual_dataframe.shape}.'
                             f'A virtual dataframe must be an nx2 shape dataframe where the columns'
                             f'correspond to row indexes and partition ids ')

        # indexes: list, partitions: list
        rows = []
        cols = None

        with tqdm(total=len(virtual_dataframe), desc=f"Computing data instances: ") as pbar:
            for i in range(len(virtual_dataframe)):
                v_row = virtual_dataframe.iloc[i]
                index, partition_name = v_row.values
                partition = self.partitions[partition_name]
                row_file = os.path.join(partition.paths.row, f'{index}_row.csv')
                row_data = pd.read_csv(row_file, skiprows=[0], names=['features', 'interactions'])
                for ware in middleware:
                    row_data = ware(row_data, cols)

                if i == 0: cols = partition.headers
                rows.append(row_data['interactions'].values)
                pbar.update()

        with alive_bar(title=f'Converting to dataframe') as bar:
            df = pd.DataFrame(rows, columns=cols, dtype=dtype)
            bar()

        return df

