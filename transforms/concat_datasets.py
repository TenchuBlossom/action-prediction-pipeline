import pandas as pd
import tools.pipeline_tools as pt
from custom_types.Data import Dataset
from collections import OrderedDict
import tools.ray_tools as rt
import ray


class Transform:

    def __init__(self, config):
        self.config = config
        self.dummy_exhausted_datasets = config.get('dummy_exhausted_datasets', True)
        self.sync_process = config.get('sync_process', True)

    def __call__(self, datasets: dict) -> dict:

        output_name = self.config.get('output_name', 'concat_dataset')
        keep_datasets = self.config.get('keep_datasets', True)
        resources = rt.init_dataset_resources(self.config, ['resources'])

        actor = Dataset.options(name=output_name, **resources).remote()
        actor_id = actor.merge_actor_data.remote([actor_id for _, actor_id in datasets.items()], output_name)
        ray.wait([actor_id], timeout=60.0)

        if keep_datasets:
            datasets[output_name] = actor
            return datasets

        datasets = OrderedDict(output_name=actor)
        return datasets



# CHECK TO SEE IF THE DUMMY DATASETS WORK AND MERGE HEADER CORRECTLY
# Put this in constructor self.old_headers = None

# if self.old_headers is not None:
#     equal = self.old_headers.equals(new_data.columns)
# else:
#     self.old_headers = new_data.columns