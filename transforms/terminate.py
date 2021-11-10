from collections import OrderedDict
from custom_types.Data import State
import ray


class Transform:

    def __init__(self, config):
        self.config = config

        # Process Flags
        self.sync_process = True

    def __call__(self, datasets: OrderedDict) -> dict:

        clean_up = self.config.get('clean_up', None)

        clean_up_ids = []
        reset_ids = []
        for dataset_name, dataset in datasets.items():
            if clean_up is not None and dataset_name in clean_up:
                clean_up_ids.append(dataset.update_state.remote(State(eligible_for_transformation=False)))
                continue

            reset_ids.append(dataset.reset.remote())

        if len(clean_up_ids) > 0: ray.wait(clean_up_ids, num_returns=len(clean_up_ids), timeout=60.0)
        ray.wait(reset_ids, num_returns=len(reset_ids), timeout=60.0)

        return datasets
