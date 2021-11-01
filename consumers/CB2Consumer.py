import pandas as pd
import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.consumer_tools as ct
import tools.py_tools as pyt
from tqdm import tqdm
import use_context
import ray


class Consumer:

    def __init__(self, config_src):

        self.config = fs.compile_config(config_src)
        self.transform_chain = ct.compile_transforms(self.config)
        self.datasets = None
        self.total_processes = None
        self.completed_processes = 0
        self.total_length = 0
        self.chunksize = pyt.get(self.config, ['consumer', 'chunksize'], 250)

        self.__compile_datasets__()

    def __compile_datasets__(self):

        datasets = dict()
        for data_src in self.config['data_sources']:
            name = data_src['name']
            src = fs.path(data_src['src'])
            sep = data_src.get('sep', None)
            metadata = data_src['metadata']
            dataset_name = name
            batch_loader = pd.read_csv(src, sep=sep, chunksize=self.chunksize, dtype=str)
            length = data_src.get('length', None)

            if data_src.get('length', None) == 'compute':
                with use_context.performance_profile("compute-csv-length", "batch"):
                    length = fs.compute_csv_len(src, name)

            self.total_length += length
            datasets[dataset_name] = pt.Dataset(**{
                'batch_loader': batch_loader,
                'length': length,
                'metadata': metadata,
                'src': src,
            }).remote()

        self.total_processes = len(datasets)
        self.datasets = datasets

    def consume(self):
        # this will
        with use_context.performance_profile("read-data", "batch"):
            total_chunks = 0
            for _, dataset in ct.transform_gate(self.datasets):
                try:
                    chunk = next(dataset.batch_loader)
                    dataset.data = chunk
                    dataset.headers = chunk.columns
                    total_chunks += len(chunk)

                except StopIteration:
                    dataset.spin_down()
                    self.completed_processes += 1

            return total_chunks

    def transform(self, show_progress=False):

        iterable = self.transform_chain

        if show_progress:
            iterable = tqdm(self.transform_chain, desc="Applying Transforms", colour="WHITE")

        for transform in iterable:
            self.datasets = transform(self.datasets)

    def processes_completed(self):
        return self.completed_processes == self.total_processes


if __name__ == '__main__':
    pass
