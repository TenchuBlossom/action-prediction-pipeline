import tools.file_system as fs
import use_context

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        with use_context.performance_profile(fs.filename(), "batch", "transforms"):
            clean_up = self.config.get('clean_up', None)
            out_datasets = dict()
            for dataset_name, dataset in datasets.items():
                dataset.headers = dataset.data.columns
                dataset.reset()

                if clean_up is not None:
                    if dataset_name not in clean_up:
                        out_datasets[dataset_name] = dataset
                    continue

                out_datasets[dataset_name] = dataset

            return out_datasets
