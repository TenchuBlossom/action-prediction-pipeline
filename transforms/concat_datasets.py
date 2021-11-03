import pandas as pd
import tools.pipeline_tools as pt
from custom_types.Data import Dataset
import ray


class Transform:

    def __init__(self, config):
        self.config = config
        self.dummy_exhausted_datasets = config.get('dummy_exhausted_datasets', True)
        self.sync_process = config.get('sync_process', True)

    def __call__(self, datasets: dict) -> dict:

        output_name = self.config.get('output_name', 'concat_dataset')
        keep_datasets = self.config.get('keep_datasets', True)

        actor = Dataset.options(num_cpus=1).remote()
        ray.wait(actor.merge_actor_data.remote([actor_id for _, actor_id in datasets]))

        states = ray.get([dataset.get_state.remote(mode='just_data') for _, dataset in datasets])
        data_to_concat = [state.data for state in states]

        new_data = pd.concat(data_to_concat)

        if keep_datasets:
            datasets[output_name] = Dataset.options(num_cpus=1).remote(**{'data': new_data})
            return datasets

        datasets = dict()
        datasets[output_name] = Dataset.options(num_cpus=1).remote(**{'data': new_data})
        return datasets



# CHECK TO SEE IF THE DUMMY DATASETS WORK AND MERGE HEADER CORRECTLY
# Put this in constructor self.old_headers = None

# if self.old_headers is not None:
#     equal = self.old_headers.equals(new_data.columns)
# else:
#     self.old_headers = new_data.columns