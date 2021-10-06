from multiprocessing import freeze_support
import tools.file_system as fs
import dask.dataframe as dd
import numpy as np

if __name__ == '__main__':
    freeze_support()

    path1 = fs.path('../data/cb2/raw/CB2_6kpc/CB2_6kpc/all_action.csv')
    path2 = fs.path('../data/cb2/raw/CB2_6kpf/CB2_6kpf/all_action.csv')

    df1 = dd.read_csv('../data/cb2/raw/CB2_5tzy_all_action.csv',  sep="\t", dtype='object')
    head = df1.head(30)
    features = np.asarray(df1.columns)
    a = 0










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