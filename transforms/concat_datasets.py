import numpy as np
import pandas as pd
import tools.consumer_tools as ct
import tools.pipeline_tools as pt


class Transform:

    def __init__(self, config):
        self.config = config
        self.old_headers = None

    def __call__(self, datasets: dict) -> dict:
        output_name = self.config.get('output_name', 'concat_dataset')
        keep_datasets = self.config.get('keep_datasets', True)

        data_to_concat = [dataset.data for _, dataset in ct.transform_gate(datasets, dummy_exhausted_datasets=True)]

        new_data = pd.concat(data_to_concat)
        if self.old_headers is not None:
            equal = self.old_headers.equals(new_data.columns)
        else:
            self.old_headers = new_data.columns

        if keep_datasets:
            datasets[output_name] = pt.Dataset(data=new_data)
            return datasets

        datasets = dict()
        datasets[output_name] = pt.Dataset(data=new_data)
        return datasets
