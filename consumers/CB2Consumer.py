import pandas as pd
import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.consumer_tools as ct
import tools.py_tools as pyt
from custom_types.Data import Dataset
from tqdm import tqdm
import ray
import asyncio
import use_context


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
            length = data_src.get('length', None)

            if data_src.get('length', None) == 'compute':
                with use_context.performance_profile("compute-csv-length", "batch"):
                    length = fs.compute_csv_len(src, name)

            self.total_length += length
            datasets[dataset_name] = Dataset.options(num_cpus=1).remote(**{
                'batch_loader_config': {
                    'filepath_or_buffer': src,
                    'sep': sep,
                    'chunksize': self.chunksize,
                    'dtype': str
                },
                'length': length,
                'metadata': metadata,
            })

        self.total_processes = len(datasets)
        self.datasets = datasets

    def consume(self):
        # this will
        with use_context.performance_profile("read-data", "batch"):

            # set processes to work async, block until all processes are done or through error if timeout
            worker_ids = [dataset.read_data.remote() for _, dataset in ct.transform_gate(self.datasets)]
            ray.wait(worker_ids, num_returns=len(self.datasets), timeout=60.0)
            total_chunks = 0

            for _, dataset in self.datasets.items():
                state = ray.get(dataset.get_state.remote(mode='just_metadata'))
                total_chunks += state.chunk_length

                if not state.batch_loader_exhausted: continue
                self.completed_processes += 1

            return total_chunks

    def transform(self, show_progress=False):
        iterable = self.transform_chain
        if show_progress:
            iterable = tqdm(self.transform_chain, desc="Applying Transforms", colour="WHITE")

        for transform in iterable:

            with use_context.performance_profile(fs.get_class_filename(transform), "batch", 'transforms'):

                dummy_exhausted_datasets, sync_process, ignore_gate = ct.get_transform_params(transform)

                # TODO Check that dummy-exhausted works when one of the datasets runs out
                datasets = ct.transform_gate(self.datasets, ignore_gate, dummy_exhausted_datasets)

                if sync_process:
                    self.datasets = transform(datasets)
                    return

                worker_ids = [dataset.transform.remote(transform) for _, dataset in datasets]
                ray.wait(worker_ids, num_returns=len(self.datasets), timeout=60.0)
                for _, dataset in self.datasets.items():
                    state = ray.get(dataset.get_state.remote())
                    break

    def processes_completed(self):
        return self.completed_processes == self.total_processes


if __name__ == '__main__':
    pass
