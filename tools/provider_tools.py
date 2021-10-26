import pandas as pd
import json
import os
import tools.file_system as fs
from mergedeep import merge, Strategy


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
        self.headers = pd.read_csv(self.paths.headers)


class VirtualDb:

    def __init__(self, pathname: str):
        self.pathname = pathname
        self.partitions = dict()

    def anchor(self):
        dir_check = lambda file: file.endswith('partition')
        for partition_name, partition_path in zip(*fs.get_dirs(self.pathname, custom_check=dir_check)):
            _, row_path = fs.get_dirs(partition_path, custom_check=lambda file: file == 'rows')
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


