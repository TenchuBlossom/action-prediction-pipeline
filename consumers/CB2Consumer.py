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

    def consume(self) -> None:

        datasets = dict()
        for data_src in self.config['data_sources']:
            name = data_src['name']
            src = fs.path(data_src['src'])
            metadata = data_src['metadata']

            data = dd.read_csv(src,  sep="\t")
            dataset_name = name
            datasets[dataset_name] = {'data': data, 'metadata': metadata, 'src': src}

        self.datasets = datasets

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
    a = 0
