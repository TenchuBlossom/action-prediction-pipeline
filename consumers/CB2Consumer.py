import pandas as pd
import tools.py_tools as pyt
import tools.file_system as fs
import os
import tools.pipeline_tools as pt
import tools.consumer_tools as ct
from multiprocessing import freeze_support


class Consumer:

    def __init__(self, config_src):

        self.config = fs.compile_config(config_src)
        self.transform_chain = ct.compile_transforms(self.config)
        self.datasets = None
        self.total_processes = None
        self.completed_processes = 0

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
            batch_loader = pd.read_csv(src, sep=sep, chunksize=chunksize, dtype=str)
            length = data_src.get('length', None)

            if data_src.get('length', None) == 'compute':
                # then compute length of dataset
                length = fs.compute_csv_len(src, name)

            datasets[dataset_name] = pt.Dataset(**{
                'batch_loader': batch_loader,
                'length': length,
                'metadata': metadata,
                'src': src,
            })

        self.total_processes = len(datasets)
        self.datasets = datasets

    def consume(self):
        # this will
        for _, dataset in ct.transform_gate(self.datasets):
            try:
                chunk = next(dataset.batch_loader)
                dataset.data = chunk
                dataset.headers = chunk.columns

            except StopIteration:
                dataset.spin_down()
                self.completed_processes += 1

    def transform(self):
        self.datasets = ct.execute_transforms(self.transform_chain, self.datasets)

    def processes_completed(self):
        return self.completed_processes == self.total_processes


if __name__ == '__main__':
    import tools.py_tools as pyt

    freeze_support()
    consumer = Consumer(fs.path('../configs/cb2/data.config.yaml'))
    consumer.consume()
    consumer.transform()
    out = consumer.provide()
    a = 0
