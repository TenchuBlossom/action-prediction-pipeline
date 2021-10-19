import tools.consumer_tools as ct

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        ignore_gate = self.config.get('ignore_gate', True)
        search_filters = self.config['filters']

        for _, dataset in ct.transform_gate(datasets, ignore_gate):
            for filter_str in search_filters:
                data = dataset['data']
                data.drop(data.filter(regex=filter_str).columns, axis=1, inplace=True)

        return datasets
