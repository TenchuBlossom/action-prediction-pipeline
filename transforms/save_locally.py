import pandas as pd
import tools.consumer_tools as ct
import tools.file_system as fs
import tools.py_tools as pyt
import os
import json

class Transform:

    def __init__(self, config):
        self.config = config
        self.index = 0
        self.partition = 0
        self.virtual_db = dict()

        dir_location = pyt.get(config, ['dir_location'])
        dir_name = pyt.get(config, ['dir_name'])
        self.dir_pathname = fs.path(os.path.join(dir_location, dir_name))
        fs.make_dir(self.dir_pathname)

    def __to_csv__(self, row, pathname, dataset_name):
        row.to_csv(os.path.join(pathname, f'{self.index}_row'), index=True)

        partition_on_columns = pyt.get(self.config, ['partition_on_columns'])
        for col_name in partition_on_columns:
            val = row[col_name]

            key_exist = self.virtual_db.get(dataset_name, None)
            if key_exist is None: self.virtual_db[dataset_name] = dict()

            key_exist = self.virtual_db[dataset_name].get(col_name, None)
            if key_exist is None: self.virtual_db[dataset_name][col_name] = dict()

            key_exist = self.virtual_db[dataset_name][col_name].get(val, None)
            if key_exist is None: self.virtual_db[dataset_name][col_name][val] = dict(index=[], partition=[], byte_size=[])

            self.virtual_db[dataset_name][col_name][val]['index'].append(self.index)
            self.virtual_db[dataset_name][col_name][val]['partition'].append(self.partition)
            self.virtual_db[dataset_name][col_name][val]['byte_size'].append(row.memory_usage())

        self.index += 1

    def __call__(self, datasets: dict):

        for name, dataset in ct.transform_gate(datasets):
            # TODO create dataset folders & create partition folder
            root_pathname = fs.make_dir(os.path.join(self.dir_pathname, name))
            partition_pathname = fs.make_dir(os.path.join(root_pathname, f'{self.partition}_partition'))
            row_pathname = fs.make_dir(os.path.join(partition_pathname, 'rows'))
            dataset.data.columns.to_series().to_csv(os.path.join(partition_pathname, 'headers'), index=False, sep='\t')
            dataset.data.apply(self.__to_csv__, args=[row_pathname, name], axis=1)

            # TODO save virtual database as json to partition
            with open(os.path.join(partition_pathname, 'virtual_db.json'), 'w') as file_obj:
                json.dump(self.virtual_db, file_obj)

            self.partition += 1

        return datasets


