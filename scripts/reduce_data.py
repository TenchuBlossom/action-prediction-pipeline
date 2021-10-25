import pandas as pd
import tools.file_system as fs
import os

chuncksizes = [500, 1500, 1000]

_, pathname = fs.make_dir('../data/cb2/test_data')
config = fs.compile_config('../configs/cb2/data.config.yaml')

for data_src, chunk in zip(config['data_sources'], chuncksizes):
    src = data_src['src']
    name = data_src['name']
    data = next(pd.read_csv(fs.path(src), sep="\t", chunksize=chunk))
    out = os.path.join(pathname, f'{name}_all_action.csv')
    data.to_csv(out, index=False, sep='\t')