import numpy as np

import tools.file_system as fs
import tools.py_tools as pyt
from custom_types.Data import State
from collections import OrderedDict
import os
import ray
from ray.util.multiprocessing import Pool
from numpy import savez_compressed
import time


def __to_csv__(args: dict):
    row = args['data']
    pathname = args['pathname']
    savez_compressed(pathname, row)
    return pathname


class Transform:

    def __init__(self, config):
        self.config = config
        self.processes = config.get('processes', 1)
        self.chunksize = config.get('chunksize', None)
        self.pool = Pool(processes=self.processes)

        self.index = 0
        self.partition = 0

        dir_location = pyt.get(config, ['dir_location'])
        dir_name = pyt.get(config, ['dir_name'])
        self.dir_pathname = fs.path(os.path.join(dir_location, dir_name))
        fs.make_dir(self.dir_pathname)

        # Processing Flags
        self.sync_process = True

    def init_distributed_saving(self, row, row_pathname: str, process_args: list, virtual_db: dict, partition_cols: list):
        # Create args for distributed saving

        process_args.append({
            'data': row,
            'pathname': os.path.join(row_pathname, f'{self.index}_row.npz'),
        })

        # Populate the virtual database
        for partition_col in partition_cols:
            col_name = partition_col['col_name']
            index = partition_col['index']
            val = row[index]
            key_exist = virtual_db.get(col_name, None)
            if key_exist is None: virtual_db[col_name] = dict()

            key_exist = virtual_db[col_name].get(val, None)
            if key_exist is None: virtual_db[col_name][val] = dict(index=[], partition=[], byte_size=[])

            virtual_db[col_name][val]['index'].append(self.index)
            virtual_db[col_name][val]['partition'].append(f'{self.partition}_partition')
            virtual_db[col_name][val]['byte_size'].append(row.nbytes)

        self.index += 1

    def __call__(self, datasets: dict) -> dict:

        # Saving per dataset is Sync. Actors can't very easily spawn there own process so we have to read data in
        state_id = [dataset.get_state.remote() for _, dataset in datasets.items()]
        ray.wait(state_id, num_returns=len(datasets), timeout=60.0)
        states = ray.get(state_id)

        for state, (_, actor) in zip(states, datasets.items()):
            root_pathname = fs.make_dir(os.path.join(self.dir_pathname, state.name))
            partition_pathname = fs.make_dir(os.path.join(root_pathname, f'{self.partition}_partition'))
            row_pathname = fs.make_dir(os.path.join(partition_pathname, 'rows'))
            virtual_db = OrderedDict()
            process_args = []

            partition_cols = []
            for col_name in pyt.get(self.config, ['partition_on_columns']):
                partition_cols.append({'col_name': col_name, 'index': state.headers.get_loc(col_name)})

            data = state.data.to_numpy()
            [self.init_distributed_saving(data[i, :], row_pathname, process_args, virtual_db, partition_cols) for i in range(len(data))]

            if self.chunksize is None:
                self.pool.map(__to_csv__, process_args)
                pass
            else:
                self.pool.imap(__to_csv__, process_args, chunksize=self.chunksize)
                pass

            # TODO save virtual database as json to partition & save header
            state.headers.to_series().to_csv(os.path.join(partition_pathname, 'headers.csv'), index=False, sep='\t')
            fs.save_json(os.path.join(partition_pathname, 'virtual_db.json'), virtual_db)

            self.index = 0

        self.partition += 1
        return datasets

    def spin_down(self):
        self.pool.close()

