import tools.consumer_tools as ct


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        value = self.config['value']
        for _, dataset in ct.transform_gate(datasets):
            dataset['data'] = dataset['data'].fillna(value)

        return datasets
