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
        )
        update_id = actor.update_state.remote(new_state)
        ray.wait([update_id], timeout=10.0)

        # state_id = actor.get_state.remote('just_data')
        # ray.wait([state_id], timeout=10.0)
        # actor_state = ray.get(state_id)
        #
        # if self.previous_batch_data is not None:
        #     equals = self.previous_batch_data.equals(actor_state.data)
        #
        # self.previous_batch_data = actor_state.data

        if keep_datasets:
            if actor_exists: return datasets
            datasets[output_name] = actor
            return datasets

        datasets = OrderedDict(output_name=actor)
        return datasets

    # def __call__(self, datasets: dict) -> dict:
    #
    #     output_name = self.config.get('output_name', 'concat_dataset')
    #     keep_datasets = self.config.get('keep_datasets', True)
    #     resources = rt.init_dataset_resources(self.config, ['resources'])
    #
    #     # get actor data
    #     state_ids = [actor_id.get_state.remote('just_data') for _, actor_id in datasets.items()]
    #     ray.wait(state_ids, timeout=10.0)
    #     states = ray.get(state_ids)
    #
    #     data_to_concat = [state.data for state in states]
    #     new_data = pd.concat(data_to_concat)
    #
    #     try:
    #         actor = ray.get_actor(name=output_name)
    #     except ValueError as e:
    #         actor = Dataset.options(name=output_name, **resources).remote()
    #
    #     state_id = actor.get_state.remote()
    #     ray.wait([state_id], timeout=60.0)
    #     state_before = ray.get(state_id)
    #
    #     actor_id = actor.merge_actor_data.remote([actor_id for _, actor_id in datasets.items()], output_name)
    #     ray.wait([actor_id], timeout=60.0)
    #
    #     state_id = actor.get_state.remote()
    #     ray.wait([state_id], timeout=60.0)
    #     state_after = ray.get(state_id)
    #     self.previous_batch_data = state_after.data
    #
    #     if self.previous_batch_data is not None:
    #         equals = self.previous_batch_data.equals(state_after.data)
    #
    #     if keep_datasets:
    #         datasets[output_name] = actor
    #         return datasets
    #
    #     datasets = OrderedDict(output_name=actor)
    #     return datasets


# CHECK TO SEE IF THE DUMMY DATASETS WORK AND MERGE HEADER CORRECTLY
# Put this in constructor self.old_headers = None

# if self.old_headers is not None:
#     equal = self.old_headers.equals(new_data.columns)
# else:
#     self.old_headers = new_data.columns