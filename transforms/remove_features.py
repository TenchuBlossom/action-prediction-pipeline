class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        search_filters = self.config['filters']

        for _, dataset in datasets.items():
            for filter_str in search_filters:
                data = dataset['data']
                data.drop(data.filter(regex=filter_str).columns, axis=1, inplace=True)

        return datasets
