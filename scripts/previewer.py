from line_profiler import LineProfiler
from mergedeep import merge, Strategy
import cProfile

def function_time():
    pr = cProfile.Profile()
    pr.enable()
    dict1 = {"key": [1, 2]}
    dict2 = {"key": [3, 4]}
    dict3 = {"key": [5, 6]}
    out = merge({}, dict1, dict2, dict3, strategy=Strategy.ADDITIVE)
    pr.disable()
    pr.print_stats(sort='time')


if __name__ == '__main__':
    function_time()
























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