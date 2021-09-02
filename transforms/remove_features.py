class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        keys = datasets.keys()
        search_params = self.config['search_params']

        for key in keys:
            data = datasets[key]['data']
            target_cols = []
            removed_cols = []
            for col in data.columns:
                finds = []
                for search_param in search_params:
                    if search_param not in col:
                        finds.append(False)
                    else:
                        finds.append(True)

                if not any(finds):
                    target_cols.append(col)
                else:
                    removed_cols.append(col)

            datasets[key]['data'] = data[target_cols]

            if 'removed_features' not in datasets[key]:
                datasets[key]['removed_features'] = {'transform-remove-features': data[removed_cols]}
            else:
                datasets[key]['removed_features']['transform-remove-features'] = data[removed_cols]

        return datasets
