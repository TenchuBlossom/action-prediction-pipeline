import tools.consumer_tools as ct


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        search_filters = self.config['filters']

        for _, dataset in ct.transform_gate(datasets):
            for filter_str in search_filters:
                dataset.data.drop(dataset.data.filter(regex=filter_str).columns, axis=1, inplace=True)

        return datasets
