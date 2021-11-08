from collections import OrderedDict
import ray


class Transform:

    def __init__(self, config):
        self.config = config

        # Process Flags
        self.sync_process = True

    def __call__(self, datasets: OrderedDict) -> dict:

        clean_up = self.config.get('clean_up', None)
        out_datasets = OrderedDict()

        terminate_ids = []
        reset_ids = []
        for dataset_name, dataset in datasets.items():
            if clean_up is not None and dataset_name in clean_up:
                terminate_ids.append(dataset.terminate.remote())
                continue

            reset_ids.append(dataset.reset.remote())
            out_datasets[dataset_name] = dataset

        if len(terminate_ids) > 0: ray.wait(terminate_ids, num_returns=len(terminate_ids), timeout=60.0)
        ray.wait(reset_ids, num_returns=len(reset_ids), timeout=60.0)

        return out_datasets
