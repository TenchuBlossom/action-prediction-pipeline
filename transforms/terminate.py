from collections import OrderedDict
import ray


class Transform:

    def __init__(self, config):
        self.config = config
        self.sync_process = True

    def __call__(self, datasets: OrderedDict):

        clean_up = self.config.get('clean_up', None)
        out_datasets = OrderedDict()

        for dataset_name, dataset in datasets.items():
            if clean_up is not None:
                if dataset_name not in clean_up:
                    out_datasets[dataset_name] = dataset
                continue

            out_datasets[dataset_name] = dataset

        reset_ids = [dataset.reset.remote() for _, dataset in datasets]
        ray.wait(reset_ids, num_returns=len(datasets), timeout=60.0)

        return out_datasets
