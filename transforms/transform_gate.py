

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:

        eligible_for_processing = self.config
        for name, dataset in datasets.items():
            if name not in eligible_for_processing:
                dataset.eligible_for_transformation = False
                continue
            dataset.eligible_for_transformation = True

        return datasets
