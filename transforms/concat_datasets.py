import pandas as pd
import tools.pipeline_tools as pt
from custom_types.Data import Dataset, State
from collections import OrderedDict
import tools.ray_tools as rt
import ray


class Transform:

    def __init__(self, config):
        self.config = config

        self.dummy_exhausted_datasets = config.get('dummy_exhausted_datasets', True)
        self.sync_process = config.get('sync_process', True)
        self.previous_batch_data = None

    def __call__(self, datasets: dict) -> dict:

        output_name = self.config.get('output_name', 'concat_dataset')
        keep_datasets = self.config.get('keep_datasets', True)
        resources = rt.init_dataset_resources(self.config, ['resources'])
        actor_exists = False

        # get actor data
        state_ids = [actor_id.get_state.remote('just_data') for name, actor_id in datasets.items() if name != output_name]
        ray.wait(state_ids, timeout=10.0)
        states = ray.get(state_ids)

        data_to_concat = [state.data for state in states]
        new_data = pd.concat(data_to_concat)

        try:
            actor = ray.get_actor(name=output_name)
            actor_exists = True
        except ValueError as e:
            actor = Dataset.options(name=output_name, **resources).remote()

        new_state = State(
            name=output_name,
            data=new_data,
            chunk_length=len(new_data),
            eligible_for_transformation=True
        )
        update_id = actor.update_state.remote(new_state)
        ray.wait([update_id], timeout=10.0)

        if keep_datasets:
            if actor_exists: return datasets
            datasets[output_name] = actor
            return datasets

        datasets = OrderedDict(output_name=actor)
        return datasets
