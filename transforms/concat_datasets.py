import pandas as pd
import tools.consumer_tools as ct

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:
        ignore_gate = self.config.get('ignore_gate', True)
        output_name = self.config.get('output_name', 'concat_dataset')
        keep_datasets = self.config.get('keep_datasets', True)

        data_to_concat = [dataset['data'] for _, dataset in ct.transform_gate(datasets, ignore_gate)]
        new_data = pd.concat(data_to_concat)

        if keep_datasets:
            datasets[output_name] = dict()
            datasets[output_name]['data'] = new_data
            return datasets

        datasets = dict()
        datasets[output_name] = dict()
        datasets[output_name]['data'] = new_data
        return datasets
