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

if __name__ == '__main__':
    freeze_support()
    # csv.field_size_limit(sys.maxsize)

    path1 = fs.path('../data/cb2/raw/CB2_5tzy_all_action.csv')
    path2 = fs.path('../data/cb2/raw/CB2_6kpf/CB2_6kpf/all_action.csv')

    reader = pd.read_csv(path1, sep='\t', chunksize=1000)

    row_idx = 0
    for chuck in reader:
        a = 0

























    # out = pyt.timeit(lambda: line_read())
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