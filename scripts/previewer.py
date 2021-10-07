from multiprocessing import freeze_support
import pandas as pd
import tools.file_system as fs
import dask.dataframe as dd
import pandas as pd
import numpy as np
import tools.py_tools as pyt
import csv
import sys
from alive_progress import alive_bar

def backspace(n):
    sys.stdout.write((b'\x08' * n).decode())

def line_read():
    with open(fs.path('../data/cb2/raw/CB2_6kpc_all_action.csv'), 'r') as read_obj:
        csv_reader = csv.reader(read_obj, delimiter='\t')
        i = 0
        for row in csv_reader:
            for e in row:
                a = e
                continue
            i += 1
            s = f'row count: {i}'
            sys.stdout.write(s)
            sys.stdout.flush()
            backspace(len(s))

if __name__ == '__main__':
    freeze_support()
    # csv.field_size_limit(sys.maxsize)

    path1 = fs.path('../data/cb2/raw/CB2_5tzy_all_action.csv')
    path2 = fs.path('../data/cb2/raw/CB2_6kpf/CB2_6kpf/all_action.csv')
    out = pyt.timeit(lambda: line_read())
    print(out)

        # data = pd.read_parquet('../data/dask-storage/CB2_CAMP=0/CB2_beta_Arrestin=AGONIST/part.6.parquet')
    # a= 0

    # data = dd.read_csv(path1, sep="\t", dtype={'Ligand_Pose': str,
    #                                            'CB2_CAMP': str, 'CB2_beta_Arrestin': str})
    # data = data.repartition(partition_size="100MB")
    #
    # out = pyt.timeit(lambda: data.to_parquet(
    #     fs.path('../data/dask-storage'),
    #     compression='snappy',
    #     partition_on=['CB2_CAMP', 'CB2_beta_Arrestin'],
    #     compute=True,
    # ), return_output=False)
    #
    # print(out)










# maxInt = sys.maxsize
#
# while True:
#     # decrease the maxInt value by factor 10
#     # as long as the OverflowError occurs.
#
#     try:
#         csv.field_size_limit(maxInt)
#         break
#     except OverflowError:
#         maxInt = int(maxInt/10)

# open file in read mode
# with open('../data/cb2/CB2_6kpc/CB2_6kpc/all.csv', 'r') as read_obj:
#     # pass the file object to reader() to get the reader object
#     csv_reader = reader(read_obj, delimiter="\t")
#     # Iterate over each row in the csv using reader object
#     for row in csv_reader:
#         # row variable is a list that represents a row in csv
#         print(row)