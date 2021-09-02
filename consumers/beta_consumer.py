import tools.file_system as fs
import os
import pandas as pd
import tools.consumer_tools as ct


class Consumer:

    def __init__(self, config_src: str):
        self.config = ct.compile_config(config_src)
        self.transform_chain = ct.compile_transforms(self.config)
        self.provider = ct.compile_provider(self.config)
        self.datasets = ct.execute_transforms(self.transform_chain, self.load())

    def load(self) -> dict:

        datasets = dict()
        for data_src in self.config['data_sources']:
            name = data_src['name']
            src = data_src['src']
            metadata = data_src['metadata']

            for file in fs.find_file_type(fs.path(src), '.csv'):
                data = pd.read_csv(os.path.join(src, file))
                dataset_name = f'{name}-{file.split(".")[0]}'
                datasets[dataset_name] = {'data': data, 'metadata': metadata, 'src': src}

        return datasets

    def provide(self) -> tuple:
        return self.provider(self.datasets)

