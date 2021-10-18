import pandas as pd
import tools.py_tools as pyt
import tools.file_system as fs
import os
import dask.dataframe as dd
import tools.consumer_tools as ct
from multiprocessing import freeze_support


class Consumer:

    def __init__(self, config_src):

        self.config = fs.compile_config(config_src)
        self.transform_chain = ct.compile_transforms(self.config)
        self.provider = ct.compile_provider(self.config)
        self.datasets = None
        self.consume_process_complete = False

        self.__compile_datasets__()

    def __compile_datasets__(self):

        chunksize = pyt.get(self.config, ['consumer', 'chunksize'], 250)

        datasets = dict()
        for data_src in self.config['data_sources']:
            name = data_src['name']
            src = fs.path(data_src['src'])
            sep = data_src.get('sep', None)
            metadata = data_src['metadata']
            dataset_name = name
            batch_loader = pd.read_csv(src, sep=sep, chunksize=chunksize)
            length = data_src.get('length', None)

            if data_src.get('len', None) == 'compute':
                # then compute length of dataset
                length = fs.compute_csv_len(src, name)

            datasets[dataset_name] = {
                'data': None,
                'batch_loader': batch_loader,
                'length': length,
                'metadata': metadata,
                'src': src
            }

        self.datasets = datasets

    def consume(self):
        # this will
        try:
            for _, dataset in self.datasets.items():
                chunk = next(dataset['batch_loader'])
                dataset['data'] = chunk

        except StopIteration:
            self.consume_process_complete = True

    def transform(self):
        self.datasets = ct.execute_transforms(self.transform_chain, self.datasets)

    def provide(self) -> tuple:
        return self.provider(self.datasets)


if __name__ == '__main__':
    import tools.py_tools as pyt

    freeze_support()
    consumer = Consumer(fs.path('../configs/cb2/data.config.yaml'))
    consumer.consume()
    consumer.transform()
    out = consumer.provide()
    a = 0
