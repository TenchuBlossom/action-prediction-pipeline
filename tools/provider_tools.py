import pandas as pd
import json
import os
import tools.file_system as fs
from mergedeep import merge, Strategy
from tqdm import tqdm
from alive_progress import alive_bar

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

        out = [self.partitions[name].database for name in partitions]

        if merge_partitions:
            out = merge({}, *out, strategy=Strategy.ADDITIVE)

        return out

    def compute(self, indexes: list, partitions: list, dtype, middleware=None):

        rows = []
        cols = None

        with tqdm(total=len(indexes), desc="Computing data instances: ") as pbar:
            for i, (idx, partition_name) in enumerate(zip(indexes, partitions)):
                partition = self.partitions[partition_name]
                row_file = os.path.join(partition.paths.row, f'{idx}_row.csv')
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

