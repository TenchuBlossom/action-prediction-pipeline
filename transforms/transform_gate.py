import tools.file_system as fs
import use_context


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:
        with use_context.performance_profile(fs.filename(), "batch", "transforms"):
            eligible_for_processing = self.config
            for name, dataset in datasets.items():
                if name not in eligible_for_processing:
                    dataset.eligible_for_transformation = False
                    continue
                dataset.eligible_for_transformation = True

            return datasets
