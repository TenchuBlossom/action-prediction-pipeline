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

# TODO Bug here Partition 98 of big_cb2 says there us row at 348 but when you check the partition
# it is not there. Either the save locally is doing something wrong or the virtual db is reading it wrong.


def __to_npz__(args: dict):
    row = args['data']
    pathname = args['pathname']
    savez_compressed(pathname, row)
    return pathname


class Transform:

    def __init__(self, config):
        self.config = config
        self.processes = config.get('processes', 1)
        self.pool = Pool(processes=self.processes)

        self.partition = 0

        dir_location = pyt.get(config, ['dir_location'])
        dir_name = pyt.get(config, ['dir_name'])
        self.dir_pathname = fs.path(os.path.join(dir_location, dir_name))
        fs.make_dir(self.dir_pathname)

        # Processing Flags
        self.sync_process = True

    def init_distributed_saving(self, state, row_pathname: str, partition_cols: list):

        virtual_db = OrderedDict()
        process_args = []

        data = state.data.to_numpy()
        for i in range(len(data)):
            # Create args for distributed saving
            row = data[i, :]
            process_args.append({
                'data': row,
                'pathname': os.path.join(row_pathname, f'{i}_row.npz'),
            })

            # Populate the virtual database
            for partition_col in partition_cols:
                col_name = partition_col['col_name']
                partition_index = partition_col['index']
                val = row[partition_index]
                key_exist = virtual_db.get(col_name, None)
                if key_exist is None: virtual_db[col_name] = dict()

                key_exist = virtual_db[col_name].get(val, None)
                if key_exist is None: virtual_db[col_name][val] = dict(index=[], partition=[], byte_size=[])

                virtual_db[col_name][val]['index'].append(i)
                virtual_db[col_name][val]['partition'].append(f'{self.partition}_partition')
                virtual_db[col_name][val]['byte_size'].append(row.nbytes)

        return process_args, virtual_db

    def __call__(self, datasets: dict) -> dict:

        # Saving per dataset is Sync. Actors can't very easily spawn there own process so we have to read data in
        state_id = [dataset.get_state.remote() for _, dataset in datasets.items()]
        ray.wait(state_id, num_returns=len(datasets), timeout=60.0)
        states = ray.get(state_id)

        for state, (_, actor) in zip(states, datasets.items()):
            root_pathname = fs.make_dir(os.path.join(self.dir_pathname, state.name))
            partition_pathname = fs.make_dir(os.path.join(root_pathname, f'{self.partition}_partition'))
            row_pathname = fs.make_dir(os.path.join(partition_pathname, 'rows'))

            partition_cols = []
            for col_name in pyt.get(self.config, ['partition_on_columns']):
                partition_cols.append({'col_name': col_name, 'index': state.headers.get_loc(col_name)})

            process_args, virtual_db = self.init_distributed_saving(state, row_pathname, partition_cols)

            self.pool.map(__to_npz__, process_args)

            # TODO save virtual database as json to partition & save header
            state.headers.to_series().to_csv(os.path.join(partition_pathname, 'headers.csv'), index=False, sep='\t')
            fs.save_json(os.path.join(partition_pathname, 'virtual_db.json'), virtual_db)

        self.partition += 1
        return datasets

    def spin_down(self):
        self.pool.close()

