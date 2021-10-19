import dask.dataframe as dd

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:

        output_name = self.config['output_name']
        data_names = list(datasets.keys())
        src = data_names.copy()

        name = data_names.pop()
        merged_dataset = datasets[name]['data']

        for name in data_names:
            subset = datasets[name]['data']
            merged_dataset = dd.concat([merged_dataset, subset])

        meta_data = 'Merged dataframe from src datasets'
        if self.config.get('keep_datasets', True):
            datasets[output_name] = {'data': merged_dataset, 'src': src, 'metadata': meta_data}

        else:
            datasets = dict()
            datasets[output_name] = {'data': merged_dataset, 'src': src, 'metadata': meta_data}

        return datasets
