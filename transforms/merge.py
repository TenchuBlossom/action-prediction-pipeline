
class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:

        output_name = self.config['output_name']
        data_names = list(datasets.keys())
        src = data_names.copy()

        marged_dataset = datasets[data_names.pop()]['data']

        for data in datasets:
            subset = datasets[data]['data']
            marged_dataset = marged_dataset.append(subset, sort=False)

        meta_data = 'Merged dataframe from src datasets'
        if self.config.get('keep_datasets', True):
            datasets[output_name] = {'data': marged_dataset, 'src': src, 'metadata': meta_data}

        else:
            datasets = dict()
            datasets[output_name] = {'data': marged_dataset, 'src': src, 'metadata': meta_data}

        return datasets
