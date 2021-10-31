import tools.consumer_tools as ct
import tools.file_system as fs
import use_context


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        with use_context.performance_profile(fs.filename(), "batch", "transforms"):
            value = self.config['value']
            for _, dataset in ct.transform_gate(datasets):
                dataset.data = dataset.data.fillna(value)

            return datasets
