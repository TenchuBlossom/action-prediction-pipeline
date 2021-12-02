import pandas as pd
import os
import tools.file_system as fs
from mergedeep import merge, Strategy
from tqdm import tqdm
from alive_progress import alive_bar
import numpy as np
import itertools
from ray.util.multiprocessing import Pool
import ray


def __sync_compute__(virtual_matrix, partitions):
    rows = []
    cols = None
    with tqdm(total=len(virtual_matrix), desc=f"Computing data instances [Sync]: ") as pbar:
        for i in range(len(virtual_matrix)):
            v_row = virtual_matrix[i]
            index, partition_name = v_row
            partition = partitions[partition_name]
            row_file = os.path.join(partition.paths.row, f'{index}_row.npz')
            row_data = np.load(row_file)['arr_0']

            if i == 0:
                cols = partition.headers

            rows.append(row_data)
            pbar.update()

    return rows, cols


def __distributed_compute__(virtual_matrix, partitions, processes=1):
    args = []
    rows = []
    cols = None
    for i in range(len(virtual_matrix)):
        v_row = virtual_matrix[i]
        index, partition_name = v_row
        partition = partitions[partition_name]
        row_file = os.path.join(partition.paths.row, f'{index}_row.npz')

        if i == 0:
            cols = partition.headers

        args.append(row_file)

    with tqdm(total=len(virtual_matrix), desc=f"Computing data instances [Distributed]: ") as pbar:
        pool = Pool(processes=processes)
        for result in pool.map(lambda file: np.load(file)['arr_0'], args):
            rows.append(result)
            pbar.update()

        pool.close()

    return rows, cols


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

    def anchor(self, check_equality=True):
        dir_check = lambda file: file.endswith('partition')
        partition_names = []
        for partition_name, partition_path in zip(*fs.get_dirs(self.pathname, custom_check=dir_check)):
            if partition_name == '98_partition':
                a = 0
            partition_names.append(partition_name)
            row_path = fs.get_dirs(partition_path, custom_check=lambda file: file == 'rows')[1][0]
            header_path, virtual_db_path = fs.find_files(partition_path, ['headers.csv', 'virtual_db.json'])[1]

            paths = dict(root=partition_path, row=row_path, headers=header_path, database=virtual_db_path)
            self.partitions[partition_name] = Partition(paths, partition_name)

        if not check_equality: return

        for pair in itertools.combinations(partition_names, 2):
            is_equal = all(self.partitions[pair[0]].headers == self.partitions[pair[1]].headers)
            if is_equal is False:
                raise ValueError(f'Provider Anchor Partition Header equality Error: Partitions {pair[0]} and {pair[1]} have different '
                                 f'headers. All partitions should have the same headers because they should belong to the same dataset')

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

    def compute(self, virtual_dataframe: pd.DataFrame, dtype, processes: int):

        if virtual_dataframe.shape[1] != 2:
            raise ValueError(f'Error Virtual Dataframe Shape: received shape {virtual_dataframe.shape}.'
                             f'A virtual dataframe must be an nx2 shape dataframe where the columns'
                             f'correspond to row indexes and partition ids ')

        virtual_matrix = virtual_dataframe.to_numpy()

        # indexes: list, partitions: list
        if processes > 1:
            matrix, column_names = __distributed_compute__(virtual_matrix, self.partitions, processes)

        else:
            matrix, column_names = __sync_compute__(virtual_matrix, self.partitions)

        # with alive_bar(title=f'Converting to dataframe', bar='classic') as bar:
        #     df = pd.DataFrame(rows, columns=cols, dtype=dtype)
        #     bar()

        return np.asarray(matrix), column_names
