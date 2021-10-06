class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        keys = datasets.keys()
        search_params = self.config['search_params']

        for key in keys:
            data = datasets[key]['data']
            datasets[key]['data'] = data.drop(search_params, axis=1, errors='ignore')

        return datasets
