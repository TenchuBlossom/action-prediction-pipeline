import ray
from custom_types.Data import State
from collections import OrderedDict


class Transform:

    def __init__(self, config):
        self.config = config
        self.sync_process = True

    def __call__(self, datasets: dict) -> dict:

        eligible_for_processing = self.config['eligible_for_processing']

        worker_ids = []
        for name, dataset in datasets.items():
            if name not in eligible_for_processing:
                worker_ids.append(dataset.update_state.remote(State(eligible_for_transformation=False)))
                continue

            worker_ids.append(dataset.update_state.remote(State(eligible_for_transformation=True)))

        ray.wait(worker_ids, num_returns=len(worker_ids), timeout=60.0)
        return datasets
