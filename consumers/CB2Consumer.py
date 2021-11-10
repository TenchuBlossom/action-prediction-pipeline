import tools.file_system as fs
import tools.consumer_tools as ct
import tools.py_tools as pyt
from custom_types.Data import Dataset
from tqdm import tqdm
from collections import OrderedDict
import tools.ray_tools as rt
from mergedeep import merge
import ray
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
        self.processes_active = False

    def spin_up_processes(self):

        datasets = OrderedDict()
        for data_src in self.config['data_sources']:
            name = data_src['name']
            src = fs.path(data_src['src'])
            sep = data_src.get('sep', None)
            metadata = data_src['metadata']
            dataset_name = name
            length = data_src.get('length', None)
            resources = rt.init_dataset_resources(data_src, ['resources'])

            if data_src.get('length', None) == 'compute':
                with use_context.performance_profile("compute-csv-length", "batch"):
                    length = fs.compute_csv_len(src, name)

            self.total_length += length
            datasets[dataset_name] = Dataset.\
                options(name=dataset_name, **resources).\
                remote(**{
                    'name': dataset_name,
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
        self.processes_active = True

    def consume(self):

        if self.processes_active is False:
            raise ValueError(f'Consumer Error: Cannot run method consume() until consumer.spin_up_processes() has been called first')

        # this will
        with use_context.performance_profile("read-data", "batch"):

            # set processes to work async, block until all processes are done or through error if timeout
            datasets = ct.transform_gate(self.datasets)
            worker_ids = [dataset.read_data.remote() for _, dataset in datasets.items()]
            ray.wait(worker_ids, num_returns=len(worker_ids), timeout=60.0)
            total_chunks = 0

            for _, dataset in datasets.items():
                state = ray.get(dataset.get_state.remote(mode='just_metadata'))
                total_chunks += state.chunk_length

                if not state.batch_loader_exhausted: continue
                self.completed_processes += 1

            return total_chunks

    def transform(self, show_progress=False, debug_states=False):
        iterable = self.transform_chain
        if show_progress:
            iterable = tqdm(self.transform_chain, desc="Applying Transforms", colour="WHITE")

        for transform in iterable:

            with use_context.performance_profile(fs.get_class_filename(transform), "batch", 'transforms'):

                dummy_exhausted_datasets, sync_process, ignore_gate = ct.get_transform_params(transform)

                # TODO Check that dummy-exhausted works when one of the datasets runs out
                datasets = ct.transform_gate(self.datasets, ignore_gate, dummy_exhausted_datasets)

                if sync_process:
                    datasets = transform(datasets)
                    self.datasets = ct.state_manager(
                        previous_state=self.datasets,
                        incoming_state=datasets,
                        transform=transform
                    )

                else:
                    worker_ids = [dataset.transform.remote(transform) for _, dataset in datasets.items()]
                    ray.wait(worker_ids, num_returns=len(datasets), timeout=60.0)

                if debug_states:
                    ct.debug_actors_states(datasets)

    def processes_completed(self):
        return self.completed_processes == self.total_processes

    def spin_down_processes(self):
        for transform in self.transform_chain:
            try:
                transform.spin_down()
            except AttributeError:
                continue


if __name__ == '__main__':
    pass
